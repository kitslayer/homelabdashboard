import asyncio
import csv
import io
import json
import os
import socket
import sqlite3
import time
import warnings
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, StreamingResponse

import httpx
import psutil

warnings.filterwarnings("ignore", category=Warning, module="httpx")

try:
    from kubernetes import client as k8s, config as k8s_cfg

    k8s_cfg.load_incluster_config()
    _v1 = k8s.CoreV1Api()
    K8S = True
except Exception:
    K8S = False

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("STATS_DB_PATH", str(BASE_DIR / "stats.db")))
DB_FALLBACK_PATH = Path("/tmp/stats_fallback.db")
PHOTOS_DIR = Path(os.getenv("PHOTO_DIR", str(BASE_DIR / "photos")))

DEFAULT_PVE_IPS: list[str] = []

HISTORY_WINDOWS = {
    "day": {"seconds": 24 * 3600, "bucket": 5 * 60},
    "week": {"seconds": 7 * 24 * 3600, "bucket": 3600},
    "month": {"seconds": 30 * 24 * 3600, "bucket": 6 * 3600},
}

WEATHER_CODES = {
    0: "Clear",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Dense drizzle",
    56: "Freezing drizzle",
    57: "Dense freezing drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    66: "Freezing rain",
    67: "Heavy freezing rain",
    71: "Light snow",
    73: "Snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Rain showers",
    81: "Heavy showers",
    82: "Violent showers",
    85: "Snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm and hail",
    99: "Severe thunderstorm and hail",
}

DEFAULT_CHANGELOG = [
    {
        "date": "2026-04-22",
        "title": "History and console rollout",
        "body": "Added SQLite-backed history, hall-of-fame summaries, CSV export, and a live request console with SSE.",
    },
    {
        "date": "2026-04-22",
        "title": "Weather and guestbook support",
        "body": "Added cached outdoor weather fetches plus guestbook submission, moderation, and rate limiting.",
    },
    {
        "date": "2026-04-22",
        "title": "Cluster-focused cleanup",
        "body": "Moved Proxmox access into environment config and cleaned up the live dashboard links and empty states.",
    },
]


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    values = [value.strip() for value in raw.split(",")]
    return [value for value in values if value] or default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except ValueError:
        return default


def _env_json(name: str, default):
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


PVE_TOKEN = os.getenv("PVE_TOKEN", "").strip()
PVE_IPS = _env_list("PVE_IPS", DEFAULT_PVE_IPS)
PVE_TIMEOUT = _env_float("PVE_TIMEOUT", 4.0)
PVE_HDR = {"Authorization": f"PVEAPIToken={PVE_TOKEN}"} if PVE_TOKEN else {}
PVE_SSH_USER = os.getenv("PVE_SSH_USER", "root").strip() or "root"
PVE_SSH_PASS = os.getenv("PVE_SSH_PASS", "").strip()
K8S_NODE_NAME = os.getenv("K8S_NODE_NAME", "").strip()
LIVE_INTERVAL_SECONDS = max(_env_int("LIVE_INTERVAL_SECONDS", 5), 1)
SNAPSHOT_INTERVAL_SECONDS = max(_env_int("SNAPSHOT_INTERVAL_SECONDS", 60), LIVE_INTERVAL_SECONDS)
CONSOLE_LIMIT = max(_env_int("CONSOLE_LIMIT", 50), 10)
WEATHER_PROXY_URL = os.getenv("WEATHER_PROXY_URL", "").strip()
WEATHER_LAT = os.getenv("WEATHER_LAT", "").strip()
WEATHER_LON = os.getenv("WEATHER_LON", "").strip()
WEATHER_LABEL = os.getenv("WEATHER_LABEL", "Outdoor weather").strip() or "Outdoor weather"
WEATHER_CACHE_SECONDS = max(_env_int("WEATHER_CACHE_SECONDS", 600), 60)
GUESTBOOK_RATE_LIMIT_SECONDS = max(_env_int("GUESTBOOK_RATE_LIMIT_SECONDS", 300), 10)
GUESTBOOK_MAX_NAME = max(_env_int("GUESTBOOK_MAX_NAME", 40), 8)
GUESTBOOK_MAX_MESSAGE = max(_env_int("GUESTBOOK_MAX_MESSAGE", 280), 80)
GUESTBOOK_PUBLIC_LIMIT = max(_env_int("GUESTBOOK_PUBLIC_LIMIT", 12), 4)
MOD_TOKEN = os.getenv("GUESTBOOK_MOD_TOKEN", "").strip()
SITE_CHANGELOG = _env_json("SITE_CHANGELOG_JSON", DEFAULT_CHANGELOG)

_db_primary_ok: bool = True
_db_primary_retry_at: float = 0.0

_prev_net = None
_prev_net_time = None
_req_count = 0
_cache: dict = {}
_metrics_clients: set = set()
_console_clients: set = set()
_loop_task: asyncio.Task | None = None
_flush_task: asyncio.Task | None = None
_last_snapshot_write = 0
_weather_cache: dict = {"configured": False}
_weather_cache_ts = 0
_weather_lock = asyncio.Lock()
_req_event_buffer: list = []
HISTORY_SUMMARY_CACHE_SECONDS = 60
HISTORY_SERIES_CACHE_SECONDS = 30
_history_summary_cache: dict | None = None
_history_summary_cache_ts: float = 0.0
_history_series_cache: dict = {}
_history_series_cache_ts: dict = {}
_pve_vm_ip_map: dict = {}
_pve_vm_ip_map_ts: float = 0.0
PVE_VM_IP_CACHE_SECONDS = 300
_pve_node_ip_map: dict = {}
_pve_node_ip_map_ts: float = 0.0
PVE_NODE_IP_CACHE_SECONDS = 300
_pve_ssh_temp_cache: dict = {}
PVE_SSH_TEMP_CACHE_SECONDS = 30


def _db_connect():
    global _db_primary_ok, _db_primary_retry_at
    if _db_primary_ok or time.time() >= _db_primary_retry_at:
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=5)
            conn.row_factory = sqlite3.Row
            if not _db_primary_ok:
                _db_primary_ok = True
                print("NFS DB recovered, switched back to primary", flush=True)
            return conn
        except Exception as exc:
            if _db_primary_ok:
                _db_primary_ok = False
                print(f"NFS DB unavailable ({exc}), switching to fallback", flush=True)
            _db_primary_retry_at = time.time() + 30
    conn = sqlite3.connect(str(DB_FALLBACK_PATH), timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _setup_db_schema(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(path), timeout=10) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS metric_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL, host TEXT NOT NULL, uptime INTEGER NOT NULL,
                cpu_pct REAL NOT NULL, mem_pct REAL NOT NULL, mem_used INTEGER NOT NULL,
                mem_total INTEGER NOT NULL, disk_pct REAL NOT NULL, disk_used INTEGER NOT NULL,
                disk_total INTEGER NOT NULL, net_in INTEGER NOT NULL, net_out INTEGER NOT NULL,
                temp REAL, requests INTEGER NOT NULL, cluster_nodes_ready INTEGER NOT NULL,
                cluster_nodes_total INTEGER NOT NULL, cluster_pods INTEGER NOT NULL,
                pve_online INTEGER, pve_total INTEGER, pve_cpu REAL, pve_mem_pct REAL, pve_avg_temp REAL
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS request_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL,
                client_ip TEXT NOT NULL, country_code TEXT, user_agent TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS guestbook_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL, name TEXT NOT NULL, message TEXT NOT NULL,
                client_ip TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                moderated_ts INTEGER, moderated_by TEXT
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_metric_snapshots_ts ON metric_snapshots(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_request_events_ts ON request_events(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_guestbook_entries_ts ON guestbook_entries(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_guestbook_entries_status ON guestbook_entries(status, ts)")


def _init_db():
    global _db_primary_ok, _db_primary_retry_at
    try:
        _setup_db_schema(DB_PATH)
    except Exception as exc:
        _db_primary_ok = False
        _db_primary_retry_at = time.time() + 30
        print(f"Primary DB init failed ({exc}), starting on fallback", flush=True)
    _setup_db_schema(DB_FALLBACK_PATH)




def _db_total_requests() -> int:
    with _db_connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM request_events").fetchone()
        return int(row["total"] if row else 0)


def _db_insert_snapshot(snapshot: dict):
    pve_total = snapshot.get("pve", {}).get("total", {})
    cluster = snapshot.get("cluster", {})
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO metric_snapshots (
                ts, host, uptime, cpu_pct, mem_pct, mem_used, mem_total,
                disk_pct, disk_used, disk_total, net_in, net_out, temp,
                requests, cluster_nodes_ready, cluster_nodes_total, cluster_pods,
                pve_online, pve_total, pve_cpu, pve_mem_pct, pve_avg_temp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot["ts"],
                snapshot["host"],
                snapshot["uptime"],
                snapshot["cpu"]["pct"],
                snapshot["mem"]["pct"],
                snapshot["mem"]["used"],
                snapshot["mem"]["total"],
                snapshot["disk"]["pct"],
                snapshot["disk"]["used"],
                snapshot["disk"]["total"],
                snapshot["net"]["in"],
                snapshot["net"]["out"],
                snapshot["temp"],
                snapshot["requests"],
                cluster.get("nodes_ready", 0),
                cluster.get("nodes_total", 0),
                cluster.get("pods", 0),
                pve_total.get("online"),
                pve_total.get("total"),
                pve_total.get("cpu"),
                pve_total.get("mem_pct"),
                pve_total.get("avg_temp"),
            ),
        )


def _db_insert_request(event: dict):
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO request_events (ts, method, path, client_ip, country_code, user_agent)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event["ts"],
                event["method"],
                event["path"],
                event["client_ip"],
                event.get("country_code"),
                event.get("user_agent"),
            ),
        )


def _db_insert_requests_batch(events: list[dict]):
    with _db_connect() as conn:
        conn.executemany(
            "INSERT INTO request_events (ts, method, path, client_ip, country_code, user_agent) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (e["ts"], e["method"], e["path"], e["client_ip"], e.get("country_code"), e.get("user_agent"))
                for e in events
            ],
        )


def _serialize_request_row(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "ts": row["ts"],
        "method": row["method"],
        "path": row["path"],
        "client_ip": _mask_ip(row["client_ip"]),
        "country_code": row["country_code"],
        "user_agent": row["user_agent"],
    }


def _db_recent_requests(limit: int) -> list[dict]:
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, method, path, client_ip, country_code, user_agent
            FROM request_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_serialize_request_row(row) for row in rows]


def _snapshot_point_from_live(snapshot: dict) -> dict:
    pve_total = snapshot.get("pve", {}).get("total", {})
    return {
        "ts": snapshot["ts"],
        "cpu_pct": snapshot["cpu"]["pct"],
        "mem_pct": snapshot["mem"]["pct"],
        "disk_pct": snapshot["disk"]["pct"],
        "temp": snapshot["temp"],
        "net_in": snapshot["net"]["in"],
        "net_out": snapshot["net"]["out"],
        "cluster_pods": snapshot["cluster"]["pods"],
        "requests": snapshot["requests"],
        "pve_cpu": pve_total.get("cpu"),
        "pve_mem_pct": pve_total.get("mem_pct"),
    }


def _db_history_points(window: str) -> dict:
    cfg = HISTORY_WINDOWS.get(window, HISTORY_WINDOWS["day"])
    cutoff = int(time.time()) - cfg["seconds"]
    bucket = cfg["bucket"]
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT
                CAST(ts / ? AS INTEGER) * ? AS bucket_ts,
                ROUND(AVG(cpu_pct), 1) AS cpu_pct,
                ROUND(AVG(mem_pct), 1) AS mem_pct,
                ROUND(AVG(disk_pct), 1) AS disk_pct,
                ROUND(AVG(temp), 1) AS temp,
                CAST(ROUND(AVG(net_in), 0) AS INTEGER) AS net_in,
                CAST(ROUND(AVG(net_out), 0) AS INTEGER) AS net_out,
                ROUND(AVG(cluster_pods), 1) AS cluster_pods,
                MAX(requests) AS requests,
                ROUND(AVG(pve_cpu), 1) AS pve_cpu,
                ROUND(AVG(pve_mem_pct), 1) AS pve_mem_pct
            FROM metric_snapshots
            WHERE ts >= ?
            GROUP BY bucket_ts
            ORDER BY bucket_ts
            """,
            (bucket, bucket, cutoff),
        ).fetchall()

    points = [
        {
            "ts": row["bucket_ts"],
            "cpu_pct": row["cpu_pct"],
            "mem_pct": row["mem_pct"],
            "disk_pct": row["disk_pct"],
            "temp": row["temp"],
            "net_in": row["net_in"],
            "net_out": row["net_out"],
            "cluster_pods": row["cluster_pods"],
            "requests": row["requests"],
            "pve_cpu": row["pve_cpu"],
            "pve_mem_pct": row["pve_mem_pct"],
        }
        for row in rows
    ]

    if not points and _cache:
        points = [_snapshot_point_from_live(_cache)]

    return {"window": window, "bucket_seconds": bucket, "points": points}


def _hall_of_fame_entry(label: str, value, unit: str, stamp: int | None, detail: str | None = None) -> dict:
    return {"label": label, "value": value, "unit": unit, "ts": stamp, "detail": detail}


def _shift_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return dt.replace(year=year, month=month, day=1)


def _db_request_rate() -> list[dict]:
    now = int(time.time())
    bucket = 3600
    cutoff = now - 24 * 3600
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT CAST(ts / ? AS INTEGER) * ? AS bucket_ts, COUNT(*) AS hits
            FROM request_events
            WHERE ts >= ?
            GROUP BY bucket_ts
            ORDER BY bucket_ts
            """,
            (bucket, bucket, cutoff),
        ).fetchall()
    hits_by_bucket = {row["bucket_ts"]: row["hits"] for row in rows}
    start = cutoff - (cutoff % bucket)
    return [{"ts": ts, "hits": int(hits_by_bucket.get(ts, 0))} for ts in range(start, now + 1, bucket)]


def _db_visitor_countries(limit: int = 12) -> dict:
    with _db_connect() as conn:
        total_row = conn.execute(
            "SELECT COUNT(*) AS total FROM request_events WHERE country_code IS NOT NULL AND country_code != ''"
        ).fetchone()
        rows = conn.execute(
            """
            SELECT country_code, COUNT(*) AS hits
            FROM request_events
            WHERE country_code IS NOT NULL AND country_code != ''
            GROUP BY country_code
            ORDER BY hits DESC, country_code ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        distinct_row = conn.execute(
            "SELECT COUNT(DISTINCT country_code) AS total FROM request_events WHERE country_code IS NOT NULL AND country_code != ''"
        ).fetchone()
    total = int(total_row["total"] if total_row else 0)
    return {
        "total_hits": total,
        "distinct_countries": int(distinct_row["total"] if distinct_row else 0),
        "countries": [
            {
                "country_code": row["country_code"],
                "hits": int(row["hits"]),
                "share": round(int(row["hits"]) / max(total, 1) * 100, 1),
            }
            for row in rows
        ],
    }


def _photo_placeholder(title: str, subtitle: str, accent: str) -> str:
    svg = f"""
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 700">
      <defs>
        <linearGradient id="g" x1="0" x2="1" y1="0" y2="1">
          <stop offset="0%" stop-color="#08111d"/>
          <stop offset="100%" stop-color="{accent}"/>
        </linearGradient>
      </defs>
      <rect width="1200" height="700" fill="url(#g)"/>
      <circle cx="1020" cy="120" r="140" fill="rgba(255,255,255,0.08)"/>
      <circle cx="240" cy="610" r="180" fill="rgba(255,255,255,0.06)"/>
      <g stroke="rgba(255,255,255,0.14)" stroke-width="2" fill="none">
        <path d="M100 520h1000"/>
        <path d="M100 420h1000"/>
        <path d="M100 320h1000"/>
      </g>
      <text x="90" y="180" fill="white" font-family="system-ui, sans-serif" font-size="78" font-weight="700">{title}</text>
      <text x="94" y="246" fill="rgba(255,255,255,0.8)" font-family="system-ui, sans-serif" font-size="28">{subtitle}</text>
      <rect x="92" y="412" width="240" height="150" rx="26" fill="rgba(255,255,255,0.08)" stroke="rgba(255,255,255,0.12)"/>
      <rect x="372" y="360" width="280" height="202" rx="26" fill="rgba(255,255,255,0.1)" stroke="rgba(255,255,255,0.14)"/>
      <rect x="692" y="280" width="416" height="282" rx="30" fill="rgba(255,255,255,0.12)" stroke="rgba(255,255,255,0.18)"/>
    </svg>
    """
    return f"data:image/svg+xml,{quote(svg.strip())}"


def _site_photos() -> list[dict]:
    photos = []
    if PHOTOS_DIR.exists():
        for path in sorted(PHOTOS_DIR.iterdir()):
            if path.is_file() and path.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
                photos.append(
                    {
                        "title": path.stem.replace("-", " ").replace("_", " ").title(),
                        "caption": "Local photo asset",
                        "src": f"/media/{path.name}",
                    }
                )
    if photos:
        return photos[:12]
    return [
        {
            "title": "Cluster racks",
            "caption": "Drop real rack photos into /home/miles/Stats/photos to replace the placeholders.",
            "src": _photo_placeholder("Cluster racks", "Default placeholder until local photos are added.", "#4f8ef7"),
        },
        {
            "title": "Node line-up",
            "caption": "Designed to keep the carousel working before image assets exist.",
            "src": _photo_placeholder("Node line-up", "Five Proxmox nodes and the K3s control plane.", "#2dd4bf"),
        },
        {
            "title": "Live console",
            "caption": "SSE-fed request history inspired by HelloESP's public feed.",
            "src": _photo_placeholder("Live console", "Traffic, history, and guestbook all in one surface.", "#f59e0b"),
        },
    ]


def _site_changelog() -> list[dict]:
    if isinstance(SITE_CHANGELOG, list) and SITE_CHANGELOG:
        cleaned = []
        for item in SITE_CHANGELOG:
            if not isinstance(item, dict):
                continue
            cleaned.append(
                {
                    "date": str(item.get("date", "")).strip() or "unknown",
                    "title": str(item.get("title", "")).strip() or "Update",
                    "body": str(item.get("body", "")).strip() or "",
                }
            )
        if cleaned:
            return cleaned[:10]
    return DEFAULT_CHANGELOG


def _serialize_guestbook_row(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "ts": row["ts"],
        "name": row["name"],
        "message": row["message"],
        "status": row["status"],
    }


def _serialize_guestbook_mod_row(row: sqlite3.Row) -> dict:
    entry = _serialize_guestbook_row(row)
    entry["client_ip"] = row["client_ip"]
    return entry


def _db_guestbook_public(limit: int) -> list[dict]:
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, name, message, status, client_ip
            FROM guestbook_entries
            WHERE status = 'approved'
            ORDER BY ts DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_serialize_guestbook_row(row) for row in rows]


def _db_guestbook_pending(limit: int) -> list[dict]:
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, name, message, status, client_ip
            FROM guestbook_entries
            WHERE status = 'pending'
            ORDER BY ts ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_serialize_guestbook_mod_row(row) for row in rows]


def _db_guestbook_last_submit_ts(client_ip: str) -> int | None:
    with _db_connect() as conn:
        row = conn.execute(
            "SELECT ts FROM guestbook_entries WHERE client_ip = ? ORDER BY ts DESC LIMIT 1",
            (client_ip,),
        ).fetchone()
    return int(row["ts"]) if row else None


def _db_guestbook_submit(entry: dict) -> int:
    with _db_connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO guestbook_entries (ts, name, message, client_ip, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (entry["ts"], entry["name"], entry["message"], entry["client_ip"]),
        )
        return int(cur.lastrowid)


def _db_guestbook_moderate(entry_id: int, action: str, moderator: str) -> bool:
    status = "approved" if action == "approve" else "rejected"
    with _db_connect() as conn:
        cur = conn.execute(
            """
            UPDATE guestbook_entries
            SET status = ?, moderated_ts = ?, moderated_by = ?
            WHERE id = ? AND status = 'pending'
            """,
            (status, int(time.time()), moderator, entry_id),
        )
        return cur.rowcount > 0


def _db_guestbook_counts() -> dict:
    with _db_connect() as conn:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending
            FROM guestbook_entries
            """
        ).fetchone()
    return {
        "approved": int(row["approved"] or 0) if row else 0,
        "pending": int(row["pending"] or 0) if row else 0,
    }


def _db_history_summary() -> dict:
    with _db_connect() as conn:
        peak_temp = conn.execute(
            "SELECT ts, temp FROM metric_snapshots WHERE temp IS NOT NULL ORDER BY temp DESC, ts DESC LIMIT 1"
        ).fetchone()
        temp_bounds = conn.execute(
            "SELECT MIN(temp) AS low, MAX(temp) AS high FROM metric_snapshots WHERE temp IS NOT NULL"
        ).fetchone()
        peak_cpu = conn.execute(
            "SELECT ts, cpu_pct FROM metric_snapshots ORDER BY cpu_pct DESC, ts DESC LIMIT 1"
        ).fetchone()
        peak_pods = conn.execute(
            "SELECT ts, cluster_pods FROM metric_snapshots ORDER BY cluster_pods DESC, ts DESC LIMIT 1"
        ).fetchone()
        longest_uptime = conn.execute(
            "SELECT ts, uptime FROM metric_snapshots ORDER BY uptime DESC, ts DESC LIMIT 1"
        ).fetchone()
        busiest_day = conn.execute(
            """
            SELECT
                strftime('%Y-%m-%d', ts, 'unixepoch', 'localtime') AS day,
                COUNT(*) AS hits
            FROM request_events
            GROUP BY day
            ORDER BY hits DESC, day DESC
            LIMIT 1
            """
        ).fetchone()
        monthly_counts = conn.execute(
            """
            SELECT
                strftime('%Y-%m', ts, 'unixepoch', 'localtime') AS month,
                COUNT(*) AS hits
            FROM request_events
            GROUP BY month
            ORDER BY month
            """
        ).fetchall()
        stats = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM metric_snapshots) AS snapshots,
                (SELECT COUNT(*) FROM request_events) AS requests
            """
        ).fetchone()

    month_map = {row["month"]: row["hits"] for row in monthly_counts}
    now = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    monthly_deltas = []
    for offset in range(11, -1, -1):
        current = _shift_months(now, -offset)
        previous = current.replace(year=current.year - 1)
        current_key = current.strftime("%Y-%m")
        previous_key = previous.strftime("%Y-%m")
        current_hits = int(month_map.get(current_key, 0))
        previous_hits = int(month_map.get(previous_key, 0))
        monthly_deltas.append(
            {
                "month": current_key,
                "label": current.strftime("%b %Y"),
                "hits": current_hits,
                "previous_hits": previous_hits,
                "delta": current_hits - previous_hits,
            }
        )

    hall = []
    if peak_temp:
        hall.append(_hall_of_fame_entry("Peak host temp", peak_temp["temp"], "C", peak_temp["ts"]))
    if temp_bounds and temp_bounds["low"] is not None and temp_bounds["high"] is not None:
        hall.append(
            _hall_of_fame_entry(
                "Temp range",
                round(temp_bounds["high"] - temp_bounds["low"], 1),
                "C",
                None,
                f"{temp_bounds['low']:.1f} C to {temp_bounds['high']:.1f} C",
            )
        )
    if peak_cpu:
        hall.append(_hall_of_fame_entry("CPU spike", peak_cpu["cpu_pct"], "%", peak_cpu["ts"]))
    if peak_pods:
        hall.append(_hall_of_fame_entry("Most running pods", peak_pods["cluster_pods"], "pods", peak_pods["ts"]))
    if longest_uptime:
        hall.append(_hall_of_fame_entry("Longest uptime", longest_uptime["uptime"], "seconds", longest_uptime["ts"]))
    if busiest_day:
        hall.append(_hall_of_fame_entry("Busiest day", busiest_day["hits"], "hits", None, busiest_day["day"]))

    return {
        "hall_of_fame": hall,
        "monthly_deltas": monthly_deltas,
        "totals": {
            "snapshots": int(stats["snapshots"] if stats else 0),
            "requests": int(stats["requests"] if stats else 0),
        },
        "visitor_geography": _db_visitor_countries(),
        "request_rate": _db_request_rate(),
        "guestbook": _db_guestbook_counts(),
        "changelog": _site_changelog(),
        "photos": _site_photos(),
    }


def _sse_message(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


async def _broadcast(clients: set, message: str):
    dead = set()
    for queue in list(clients):
        try:
            queue.put_nowait(message)
        except Exception:
            dead.add(queue)
    clients.difference_update(dead)


def _should_track_request(request: Request, status_code: int) -> bool:
    path = request.url.path
    if request.method != "GET" or status_code >= 500:
        return False
    if path.startswith("/api/") or path.startswith("/media/"):
        return False
    return path in {"/", "/history", "/console"}


def _request_country_code(request: Request) -> str | None:
    for header in ("cf-ipcountry", "x-vercel-ip-country", "x-country-code"):
        code = request.headers.get(header, "").strip().upper()
        if code and code not in {"XX", "T1"}:
            return code
    return None


def _request_client_ip(request: Request) -> str:
    for header in ("cf-connecting-ip", "x-real-ip", "x-forwarded-for"):
        value = request.headers.get(header, "").strip()
        if value:
            return value.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _mask_ip(ip: str) -> str:
    if not ip or ip == "unknown":
        return "unknown"
    if ":" in ip:
        parts = [part for part in ip.split(":") if part]
        return f"{':'.join(parts[:2])}::" if parts else "::"
    parts = ip.split(".")
    if len(parts) == 4:
        return f"{parts[0]}.{parts[1]}.{parts[2]}.x"
    return ip


def _request_event(request: Request) -> dict:
    return {
        "ts": int(time.time()),
        "method": request.method,
        "path": request.url.path,
        "client_ip": _request_client_ip(request),
        "country_code": _request_country_code(request),
        "user_agent": (request.headers.get("user-agent", "") or "")[:240],
    }


async def _flush_loop():
    global _req_event_buffer, _db_primary_ok, _db_primary_retry_at
    while True:
        await asyncio.sleep(0.5)
        if not _req_event_buffer:
            continue
        batch = _req_event_buffer[:]
        del _req_event_buffer[:]
        try:
            await asyncio.to_thread(_db_insert_requests_batch, batch)
        except Exception as exc:
            print(f"flush error: {exc}", flush=True)
            if _db_primary_ok:
                _db_primary_ok = False
                _db_primary_retry_at = time.time() + 30
                print("NFS write failed, switching to fallback", flush=True)


async def _record_request(event: dict):
    _req_event_buffer.append(event)
    public_event = dict(event)
    public_event["client_ip"] = _mask_ip(event["client_ip"])
    await _broadcast(_console_clients, _sse_message(public_event))


def _sanitize_guestbook_text(value: str, max_len: int) -> str:
    cleaned = " ".join((value or "").strip().split())
    return cleaned[:max_len]


def _moderation_token(request: Request) -> str:
    return request.headers.get("x-mod-token", "").strip()


def _require_mod_token(request: Request) -> str:
    if not MOD_TOKEN:
        raise HTTPException(status_code=503, detail="Moderation is not configured.")
    provided = _moderation_token(request)
    if provided != MOD_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid moderation token.")
    return "moderator"


def _weather_params() -> dict:
    params = {
        "current": "temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,is_day,wind_speed_10m",
        "timezone": "auto",
    }
    if WEATHER_LAT and WEATHER_LON:
        params["latitude"] = WEATHER_LAT
        params["longitude"] = WEATHER_LON
    return params


def _normalize_weather(payload: dict, source: str) -> dict:
    current = payload.get("current") or payload.get("current_weather") or payload
    if not isinstance(current, dict):
        return {"configured": False, "label": WEATHER_LABEL}

    code = current.get("weather_code")
    if code is None and "weathercode" in current:
        code = current.get("weathercode")
    temp = current.get("temperature_2m")
    if temp is None and "temperature" in current:
        temp = current.get("temperature")
    wind = current.get("wind_speed_10m")
    if wind is None and "windspeed" in current:
        wind = current.get("windspeed")
    humidity = current.get("relative_humidity_2m")
    apparent = current.get("apparent_temperature")
    is_day = current.get("is_day")

    return {
        "configured": True,
        "label": WEATHER_LABEL,
        "source": source,
        "temperature_c": temp,
        "apparent_c": apparent,
        "humidity_pct": humidity,
        "wind_kph": wind,
        "code": code,
        "summary": WEATHER_CODES.get(int(code), "Weather") if code is not None else "Weather",
        "is_day": is_day,
        "ts": int(time.time()),
    }


async def _fetch_weather() -> dict:
    global _weather_cache, _weather_cache_ts

    async with _weather_lock:
        now = time.time()
        if now - _weather_cache_ts < WEATHER_CACHE_SECONDS and _weather_cache:
            return _weather_cache

        try:
            params = _weather_params()
            if WEATHER_PROXY_URL:
                async with httpx.AsyncClient(timeout=8) as client:
                    response = await client.get(WEATHER_PROXY_URL, params=params)
            elif WEATHER_LAT and WEATHER_LON:
                async with httpx.AsyncClient(timeout=8) as client:
                    response = await client.get("https://api.open-meteo.com/v1/forecast", params=params)
            else:
                _weather_cache = {"configured": False, "label": WEATHER_LABEL}
                _weather_cache_ts = now
                return _weather_cache

            response.raise_for_status()
            _weather_cache = _normalize_weather(
                response.json(),
                "proxy" if WEATHER_PROXY_URL else "open-meteo",
            )
        except Exception as exc:
            _weather_cache = {
                "configured": bool(WEATHER_PROXY_URL or (WEATHER_LAT and WEATHER_LON)),
                "label": WEATHER_LABEL,
                "error": str(exc),
            }
        _weather_cache_ts = now
        return _weather_cache


def _safe_photo_path(filename: str) -> Path:
    candidate = (PHOTOS_DIR / filename).resolve()
    if not str(candidate).startswith(str(PHOTOS_DIR.resolve())):
        raise HTTPException(status_code=404, detail="Not found")
    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(status_code=404, detail="Not found")
    return candidate


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _req_count, _loop_task, _flush_task

    _init_db()
    _req_count = await asyncio.to_thread(_db_total_requests)
    _loop_task = asyncio.create_task(_loop())
    _flush_task = asyncio.create_task(_flush_loop())
    try:
        yield
    finally:
        if _loop_task:
            _loop_task.cancel()
            with suppress(asyncio.CancelledError):
                await _loop_task
        if _flush_task:
            _flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await _flush_task
        if _req_event_buffer:
            with suppress(Exception):
                _db_insert_requests_batch(_req_event_buffer)


app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def _count(request: Request, call_next):
    global _req_count

    response = await call_next(request)
    if _should_track_request(request, response.status_code):
        _req_count += 1
        asyncio.create_task(_record_request(_request_event(request)))
    return response


def _cpu_temp():
    try:
        temps = psutil.sensors_temperatures()
        for key in ("coretemp", "k10temp", "cpu_thermal", "acpitz"):
            if key in temps and temps[key]:
                return round(max(item.current for item in temps[key]), 1)
    except Exception:
        pass
    for index in range(8):
        try:
            with open(f"/sys/class/thermal/thermal_zone{index}/temp") as handle:
                return round(int(handle.read().strip()) / 1000, 1)
        except Exception:
            continue
    return None


def _k8s_stats():
    if not K8S:
        return {"nodes_ready": 0, "nodes_total": 0, "pods": 0, "nodes": []}
    try:
        k8s_nodes = _v1.list_node()
        node_list = []
        for node in k8s_nodes.items:
            ready = any(
                c.type == "Ready" and c.status == "True"
                for c in (node.status.conditions or [])
            )
            internal_ip = next(
                (a.address for a in (node.status.addresses or []) if a.type == "InternalIP"),
                None,
            )
            node_list.append({"name": node.metadata.name, "ready": ready, "internal_ip": internal_ip})
        total = len(node_list)
        ready_count = sum(1 for n in node_list if n["ready"])
    except Exception:
        total = ready_count = 0
        node_list = []
    try:
        running = sum(1 for pod in _v1.list_pod_for_all_namespaces().items if pod.status.phase == "Running")
    except Exception:
        running = 0
    return {"nodes_ready": ready_count, "nodes_total": total, "pods": running, "nodes": node_list}


async def _pve_node_temp_ssh(host: str) -> float | None:
    global _pve_ssh_temp_cache
    now = time.time()
    cached = _pve_ssh_temp_cache.get(host)
    if cached is not None and now - cached[1] < PVE_SSH_TEMP_CACHE_SECONDS:
        return cached[0]
    if not PVE_SSH_PASS:
        return None
    result = None
    try:
        proc = await asyncio.create_subprocess_exec(
            "sshpass", "-p", PVE_SSH_PASS,
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=4",
            "-o", "BatchMode=no",
            f"{PVE_SSH_USER}@{host}",
            "for d in /sys/class/hwmon/hwmon*/; do name=$(cat \"${d}name\" 2>/dev/null); if [ \"$name\" = \"coretemp\" ] || [ \"$name\" = \"k10temp\" ]; then cat \"${d}\"temp*_input 2>/dev/null; fi; done",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=6)
        values = []
        for line in stdout.decode().splitlines():
            line = line.strip()
            if line.isdigit():
                c = int(line) / 1000.0
                if 30.0 <= c <= 110.0:
                    values.append(c)
        result = round(max(values), 1) if values else None
    except Exception:
        pass
    _pve_ssh_temp_cache[host] = (result, now)
    return result


async def _pve_node_temp(client: httpx.AsyncClient, base: str, name: str, host_ip: str | None = None):
    try:
        response = await client.get(f"{base}/api2/json/nodes/{name}/status", headers=PVE_HDR)
        if response.status_code == 200:
            state = response.json().get("data", {}).get("thermalstate")
            if state:
                import re
                match = re.search(r"[\d.]+", str(state))
                if match:
                    return float(match.group())
    except Exception:
        pass
    if host_ip:
        return await _pve_node_temp_ssh(host_ip)
    return None


async def _pve_build_vm_name_map(client: httpx.AsyncClient, base: str, raw_nodes: list) -> dict:
    """Returns {vm_name_lower: pve_host_name} for all running VMs."""
    mapping: dict = {}
    for node in raw_nodes:
        node_name = node["node"]
        try:
            resp = await client.get(f"{base}/api2/json/nodes/{node_name}/qemu", headers=PVE_HDR)
            if resp.status_code != 200:
                continue
            for vm in resp.json().get("data", []):
                if vm.get("status") == "running" and vm.get("name"):
                    mapping[vm["name"].lower()] = node_name
        except Exception:
            continue
    return mapping


async def _pve_stats():
    global _pve_vm_ip_map, _pve_vm_ip_map_ts, _pve_node_ip_map, _pve_node_ip_map_ts

    if not PVE_TOKEN or not PVE_IPS:
        return {"nodes": [], "total": {}, "configured": False, "vm_ip_map": {}}

    for ip in PVE_IPS:
        base = f"https://{ip}:8006"
        try:
            async with httpx.AsyncClient(verify=False, timeout=PVE_TIMEOUT) as client:
                response = await client.get(f"{base}/api2/json/nodes", headers=PVE_HDR)
                if response.status_code != 200:
                    continue
                raw_nodes = response.json().get("data", [])

                now = time.time()
                if not _pve_node_ip_map or now - _pve_node_ip_map_ts > PVE_NODE_IP_CACHE_SECONDS:
                    try:
                        cs = await client.get(f"{base}/api2/json/cluster/status", headers=PVE_HDR)
                        if cs.status_code == 200:
                            _pve_node_ip_map = {
                                e["name"]: e["ip"]
                                for e in cs.json().get("data", [])
                                if e.get("type") == "node" and e.get("ip")
                            }
                            _pve_node_ip_map_ts = now
                    except Exception:
                        pass

                temps = await asyncio.gather(*[
                    _pve_node_temp(client, base, node["node"], _pve_node_ip_map.get(node["node"]))
                    for node in raw_nodes
                ])

                now = time.time()
                if now - _pve_vm_ip_map_ts > PVE_VM_IP_CACHE_SECONDS:
                    _pve_vm_ip_map = await _pve_build_vm_name_map(client, base, raw_nodes)
                    _pve_vm_ip_map_ts = now

                nodes = []
                for node, temp in zip(raw_nodes, temps):
                    maxmem = max(node.get("maxmem", 1), 1)
                    nodes.append(
                        {
                            "name": node["node"],
                            "status": node.get("status", "unknown"),
                            "cpu": round(node.get("cpu", 0) * 100, 1),
                            "mem_pct": round(node.get("mem", 0) / maxmem * 100, 1),
                            "mem_used": node.get("mem", 0),
                            "mem_total": maxmem,
                            "uptime": node.get("uptime", 0),
                            "temp": temp,
                        }
                    )
                nodes.sort(key=lambda item: item["name"])

                online = [node for node in nodes if node["status"] == "online"]
                total_cpu = round(sum(node["cpu"] for node in online) / max(len(online), 1), 1) if online else 0
                total_mem_used = sum(node["mem_used"] for node in online)
                total_mem_total = sum(node["mem_total"] for node in online)
                total_mem_pct = round(total_mem_used / max(total_mem_total, 1) * 100, 1)
                known_temps = [node["temp"] for node in online if node["temp"] is not None]

                return {
                    "nodes": nodes,
                    "total": {
                        "online": len(online),
                        "total": len(nodes),
                        "cpu": total_cpu,
                        "mem_pct": total_mem_pct,
                        "mem_used": total_mem_used,
                        "mem_total": total_mem_total,
                        "avg_temp": round(sum(known_temps) / len(known_temps), 1) if known_temps else None,
                    },
                    "configured": True,
                    "vm_ip_map": _pve_vm_ip_map,
                }
        except Exception:
            continue
    return {
        "nodes": [],
        "total": {},
        "configured": bool(PVE_TOKEN and PVE_IPS),
        "vm_ip_map": _pve_vm_ip_map,
    }


async def _collect():
    global _prev_net, _prev_net_time

    loop = asyncio.get_running_loop()
    cpu = await loop.run_in_executor(None, lambda: psutil.cpu_percent(interval=0.2))
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    net = psutil.net_io_counters()
    now = time.time()

    net_in = net_out = 0.0
    if _prev_net and _prev_net_time:
        delta = now - _prev_net_time
        if delta > 0:
            net_in = (net.bytes_recv - _prev_net.bytes_recv) / delta
            net_out = (net.bytes_sent - _prev_net.bytes_sent) / delta
    _prev_net, _prev_net_time = net, now

    k8s_data, pve = await asyncio.gather(loop.run_in_executor(None, _k8s_stats), _pve_stats())

    vm_name_map = pve.get("vm_ip_map", {})
    pve_host_temps = {n["name"]: n["temp"] for n in pve.get("nodes", [])}
    for k3s_node in k8s_data.get("nodes", []):
        pve_host = vm_name_map.get(k3s_node.get("name", "").lower())
        k3s_node["pve_host"] = pve_host
        k3s_node["pve_host_temp"] = pve_host_temps.get(pve_host) if pve_host else None

    return {
        "ts": int(now),
        "host": socket.gethostname(),
        "k8s_node": K8S_NODE_NAME,
        "uptime": int(now - psutil.boot_time()),
        "cpu": {"pct": round(cpu, 1), "cores": psutil.cpu_count()},
        "mem": {"pct": round(mem.percent, 1), "used": mem.used, "total": mem.total},
        "disk": {"pct": round(disk.percent, 1), "used": disk.used, "total": disk.total},
        "net": {"in": int(net_in), "out": int(net_out)},
        "temp": _cpu_temp(),
        "cluster": k8s_data,
        "pve": pve,
        "requests": _req_count,
    }


async def _loop():
    global _cache, _last_snapshot_write

    while True:
        try:
            snapshot = await _collect()
            _cache = snapshot
            await _broadcast(_metrics_clients, _sse_message(snapshot))

            if snapshot["ts"] - _last_snapshot_write >= SNAPSHOT_INTERVAL_SECONDS:
                try:
                    await asyncio.to_thread(_db_insert_snapshot, snapshot)
                    _last_snapshot_write = snapshot["ts"]
                except Exception as exc:
                    print(f"snapshot write error: {exc}", flush=True)
        except Exception as exc:
            print(f"collect error: {exc}", flush=True)
        await asyncio.sleep(LIVE_INTERVAL_SECONDS)


def _stream_from_queue(queue: asyncio.Queue, clients: set):
    async def gen():
        try:
            while True:
                try:
                    yield await asyncio.wait_for(queue.get(), timeout=25)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            clients.discard(queue)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/stream")
async def stream():
    queue = asyncio.Queue(maxsize=10)
    _metrics_clients.add(queue)
    if _cache:
        queue.put_nowait(_sse_message(_cache))
    return _stream_from_queue(queue, _metrics_clients)


@app.get("/api/console/stream")
async def console_stream():
    queue = asyncio.Queue(maxsize=CONSOLE_LIMIT)
    _console_clients.add(queue)
    return _stream_from_queue(queue, _console_clients)


@app.get("/api/metrics")
async def metrics():
    return _cache


@app.get("/api/weather")
async def weather():
    return await _fetch_weather()


@app.get("/api/console/recent")
async def console_recent(limit: int = CONSOLE_LIMIT):
    safe_limit = max(1, min(limit, 200))
    return {"events": await asyncio.to_thread(_db_recent_requests, safe_limit)}


@app.get("/api/history/series")
async def history_series(window: str = "day"):
    global _history_series_cache, _history_series_cache_ts
    safe_window = window if window in HISTORY_WINDOWS else "day"
    now = time.time()
    if safe_window in _history_series_cache and now - _history_series_cache_ts.get(safe_window, 0) < HISTORY_SERIES_CACHE_SECONDS:
        return _history_series_cache[safe_window]
    result = await asyncio.to_thread(_db_history_points, safe_window)
    _history_series_cache[safe_window] = result
    _history_series_cache_ts[safe_window] = now
    return result


@app.get("/api/history/summary")
async def history_summary():
    global _history_summary_cache, _history_summary_cache_ts
    now = time.time()
    if _history_summary_cache is not None and now - _history_summary_cache_ts < HISTORY_SUMMARY_CACHE_SECONDS:
        return _history_summary_cache
    result = await asyncio.to_thread(_db_history_summary)
    _history_summary_cache = result
    _history_summary_cache_ts = now
    return result


@app.get("/api/history/export.csv")
async def history_export(window: str = "day"):
    safe_window = window if window in HISTORY_WINDOWS else "day"
    payload = await asyncio.to_thread(_db_history_points, safe_window)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(
        ["ts", "cpu_pct", "mem_pct", "disk_pct", "temp", "net_in", "net_out", "cluster_pods", "requests"]
    )
    for point in payload["points"]:
        writer.writerow(
            [
                point["ts"],
                point["cpu_pct"],
                point["mem_pct"],
                point["disk_pct"],
                point["temp"],
                point["net_in"],
                point["net_out"],
                point["cluster_pods"],
                point["requests"],
            ]
        )
    return PlainTextResponse(
        buffer.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="history-{safe_window}.csv"'},
    )


@app.get("/api/guestbook")
async def guestbook_public(limit: int = GUESTBOOK_PUBLIC_LIMIT):
    safe_limit = max(1, min(limit, 50))
    entries = await asyncio.to_thread(_db_guestbook_public, safe_limit)
    return {"entries": entries, "counts": await asyncio.to_thread(_db_guestbook_counts)}


@app.post("/api/guestbook", status_code=201)
async def guestbook_submit(payload: dict, request: Request):
    name = _sanitize_guestbook_text(str(payload.get("name", "")), GUESTBOOK_MAX_NAME)
    message = _sanitize_guestbook_text(str(payload.get("message", "")), GUESTBOOK_MAX_MESSAGE)
    if not name:
        raise HTTPException(status_code=400, detail="Name is required.")
    if not message:
        raise HTTPException(status_code=400, detail="Message is required.")

    client_ip = _request_client_ip(request)
    last_ts = await asyncio.to_thread(_db_guestbook_last_submit_ts, client_ip)
    now = int(time.time())
    if last_ts and now - last_ts < GUESTBOOK_RATE_LIMIT_SECONDS:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limited. Try again in {GUESTBOOK_RATE_LIMIT_SECONDS - (now - last_ts)} seconds.",
        )

    entry_id = await asyncio.to_thread(
        _db_guestbook_submit,
        {"ts": now, "name": name, "message": message, "client_ip": client_ip},
    )
    return {
        "ok": True,
        "id": entry_id,
        "status": "pending",
        "message": "Entry submitted for moderation.",
    }


@app.get("/api/guestbook/moderation")
async def guestbook_moderation(request: Request, limit: int = 50):
    _require_mod_token(request)
    safe_limit = max(1, min(limit, 200))
    return {"entries": await asyncio.to_thread(_db_guestbook_pending, safe_limit)}


@app.post("/api/guestbook/{entry_id}/moderate")
async def guestbook_moderate(entry_id: int, payload: dict, request: Request):
    moderator = _require_mod_token(request)
    action = str(payload.get("action", "")).strip().lower()
    if action not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="Action must be approve or reject.")
    changed = await asyncio.to_thread(_db_guestbook_moderate, entry_id, action, moderator)
    if not changed:
        raise HTTPException(status_code=404, detail="Pending entry not found.")
    return {"ok": True, "id": entry_id, "status": "approved" if action == "approve" else "rejected"}


@app.get("/media/{filename:path}")
async def media(filename: str):
    return FileResponse(_safe_photo_path(filename))


@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/history")
async def history_page():
    return FileResponse(BASE_DIR / "history.html")


@app.get("/console")
async def console_page():
    return FileResponse(BASE_DIR / "console.html")
