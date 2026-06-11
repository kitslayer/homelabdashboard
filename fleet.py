"""Fleet subsystem: multi-host metrics ingest, history, alerts, topology.

Separate from the public dashboard so the existing site at stats.milescoviello.com is
untouched. All routes are auth-gated (bearer token). Schema lives in its own
fleet_* tables sharing the homelab-stats Postgres.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import io
import json
import os
import secrets
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import psycopg
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = os.environ["DATABASE_URL"]

AGENT_VERSION_SUPPORTED = "0.1.x"
SAMPLE_RETENTION_RAW_SECONDS = int(os.getenv("FLEET_SAMPLE_RETENTION_RAW", str(7 * 24 * 3600)))
SAMPLE_RETENTION_ROLLUP_SECONDS = int(os.getenv("FLEET_SAMPLE_RETENTION_ROLLUP", str(90 * 24 * 3600)))
ALERT_EVAL_INTERVAL_SECONDS = int(os.getenv("FLEET_ALERT_EVAL_INTERVAL", "30"))
HOST_DOWN_AFTER_SECONDS = int(os.getenv("FLEET_HOST_DOWN_AFTER", "120"))
FLOORPLAN_DIR = Path(os.getenv("FLEET_FLOORPLAN_DIR", "/data/floorplans"))


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _admin_hash() -> str | None:
    raw = os.getenv("FLEET_ADMIN_TOKEN", "").strip()
    if raw:
        return _hash_token(raw)
    h = os.getenv("FLEET_ADMIN_TOKEN_HASH", "").strip().lower()
    return h or None


def _bootstrap_hash() -> str | None:
    """Optional separate token agents use only for registration."""
    raw = os.getenv("FLEET_BOOTSTRAP_TOKEN", "").strip()
    if raw:
        return _hash_token(raw)
    h = os.getenv("FLEET_BOOTSTRAP_TOKEN_HASH", "").strip().lower()
    return h or None


def _db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True, connect_timeout=5)


def _ddl(cur, sql: str) -> None:
    with suppress(psycopg.errors.DuplicateObject, psycopg.errors.UniqueViolation):
        cur.execute(sql)


SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS fleet_hosts (
        id              BIGSERIAL PRIMARY KEY,
        host_uuid       TEXT UNIQUE NOT NULL,
        hostname        TEXT NOT NULL,
        display_name    TEXT,
        ip              TEXT,
        tailscale_ip    TEXT,
        os              TEXT,
        distro          TEXT,
        kernel          TEXT,
        arch            TEXT,
        agent_version   TEXT,
        tags            TEXT,
        location_tag    TEXT,
        lat             DOUBLE PRECISION,
        lon             DOUBLE PRECISION,
        floorplan_id    BIGINT,
        floorplan_x     DOUBLE PRECISION,
        floorplan_y     DOUBLE PRECISION,
        topology_group  TEXT,
        notes           TEXT,
        registered_at   BIGINT NOT NULL,
        last_seen       BIGINT,
        enabled         BOOLEAN NOT NULL DEFAULT TRUE
    )""",
    """CREATE TABLE IF NOT EXISTS fleet_api_keys (
        id          BIGSERIAL PRIMARY KEY,
        host_id     BIGINT REFERENCES fleet_hosts(id) ON DELETE CASCADE,
        key_hash    TEXT UNIQUE NOT NULL,
        kind        TEXT NOT NULL,
        label       TEXT,
        created_at  BIGINT NOT NULL,
        last_used   BIGINT
    )""",
    """CREATE TABLE IF NOT EXISTS fleet_samples (
        id       BIGSERIAL PRIMARY KEY,
        host_id  BIGINT NOT NULL REFERENCES fleet_hosts(id) ON DELETE CASCADE,
        ts       BIGINT NOT NULL,
        payload  JSONB NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_fleet_samples_host_ts ON fleet_samples (host_id, ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_fleet_samples_ts ON fleet_samples (ts)",
    """CREATE TABLE IF NOT EXISTS fleet_alert_rules (
        id          BIGSERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        host_filter TEXT NOT NULL DEFAULT 'all',
        metric      TEXT NOT NULL,
        op          TEXT NOT NULL,
        threshold   DOUBLE PRECISION,
        duration_s  INTEGER NOT NULL DEFAULT 60,
        severity    TEXT NOT NULL DEFAULT 'warning',
        message_tpl TEXT,
        enabled     BOOLEAN NOT NULL DEFAULT TRUE,
        created_at  BIGINT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS fleet_alerts (
        id          BIGSERIAL PRIMARY KEY,
        rule_id     BIGINT REFERENCES fleet_alert_rules(id) ON DELETE SET NULL,
        host_id     BIGINT REFERENCES fleet_hosts(id) ON DELETE SET NULL,
        severity    TEXT NOT NULL,
        message     TEXT NOT NULL,
        fired_at    BIGINT NOT NULL,
        cleared_at  BIGINT,
        last_value  DOUBLE PRECISION
    )""",
    "CREATE INDEX IF NOT EXISTS idx_fleet_alerts_active ON fleet_alerts (fired_at DESC) WHERE cleared_at IS NULL",
    "CREATE INDEX IF NOT EXISTS idx_fleet_alerts_fired ON fleet_alerts (fired_at DESC)",
    """CREATE TABLE IF NOT EXISTS fleet_floorplans (
        id          BIGSERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        image_path  TEXT,
        width       INTEGER NOT NULL DEFAULT 1000,
        height      INTEGER NOT NULL DEFAULT 600,
        is_default  BOOLEAN NOT NULL DEFAULT FALSE,
        created_at  BIGINT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS fleet_topology_edges (
        id        BIGSERIAL PRIMARY KEY,
        a_host_id BIGINT NOT NULL REFERENCES fleet_hosts(id) ON DELETE CASCADE,
        b_host_id BIGINT NOT NULL REFERENCES fleet_hosts(id) ON DELETE CASCADE,
        kind      TEXT NOT NULL,
        label     TEXT
    )""",
]

_DEFAULT_RULES: list[dict[str, Any]] = [
    {"name": "Host down", "host_filter": "all", "metric": "host_down", "op": "gt", "threshold": 0, "duration_s": 0, "severity": "critical"},
    {"name": "CPU > 90%", "host_filter": "all", "metric": "cpu.pct", "op": "gt", "threshold": 90, "duration_s": 300, "severity": "warning"},
    {"name": "Memory > 90%", "host_filter": "all", "metric": "mem.pct", "op": "gt", "threshold": 90, "duration_s": 300, "severity": "warning"},
    {"name": "Disk > 85% on /", "host_filter": "all", "metric": "disk_root.pct", "op": "gt", "threshold": 85, "duration_s": 300, "severity": "warning"},
    {"name": "CPU temp > 90C", "host_filter": "all", "metric": "cpu.temp", "op": "gt", "threshold": 90, "duration_s": 60, "severity": "critical"},
    {"name": "GPU temp > 90C", "host_filter": "all", "metric": "gpu.temp_max", "op": "gt", "threshold": 90, "duration_s": 60, "severity": "warning"},
    {"name": "ZFS pool degraded", "host_filter": "all", "metric": "zfs.degraded", "op": "gt", "threshold": 0, "duration_s": 60, "severity": "critical"},
    {"name": "Battery low", "host_filter": "all", "metric": "battery.pct_on_battery", "op": "lt", "threshold": 15, "duration_s": 60, "severity": "warning"},
]


def setup_schema() -> None:
    deadline = time.time() + 60
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            with _db() as conn, conn.cursor() as cur:
                for stmt in SCHEMA_STATEMENTS:
                    _ddl(cur, stmt)
                row = cur.execute("SELECT COUNT(*) AS n FROM fleet_alert_rules").fetchone()
                if row and row["n"] == 0:
                    now = int(time.time())
                    for r in _DEFAULT_RULES:
                        cur.execute(
                            """INSERT INTO fleet_alert_rules
                               (name, host_filter, metric, op, threshold, duration_s, severity, enabled, created_at)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)""",
                            (r["name"], r["host_filter"], r["metric"], r["op"],
                             r["threshold"], r["duration_s"], r["severity"], now),
                        )
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"fleet DB init waiting ({exc})", flush=True)
            time.sleep(2)
    raise RuntimeError(f"fleet DB init failed: {last_exc}")


# ---------------------------------------------------------------------------
# Auth dependencies
# ---------------------------------------------------------------------------


def _extract_bearer(request: Request, authorization: str | None) -> str:
    if authorization:
        parts = authorization.strip().split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return parts[1].strip()
    hdr = request.headers.get("x-fleet-token", "").strip()
    if hdr:
        return hdr
    return ""


async def require_admin(
    request: Request,
    authorization: str | None = Header(default=None),
) -> str:
    expected = _admin_hash()
    if not expected:
        raise HTTPException(status_code=503, detail="fleet admin token not configured")
    token = _extract_bearer(request, authorization)
    if not token:
        raise HTTPException(status_code=401, detail="missing bearer token")
    if not hmac.compare_digest(_hash_token(token), expected):
        raise HTTPException(status_code=403, detail="invalid token")
    return token


async def require_bootstrap(
    request: Request,
    authorization: str | None = Header(default=None),
) -> str:
    """Accepts either the bootstrap token or the admin token."""
    token = _extract_bearer(request, authorization)
    if not token:
        raise HTTPException(status_code=401, detail="missing bearer token")
    hashed = _hash_token(token)
    if (b := _bootstrap_hash()) and hmac.compare_digest(hashed, b):
        return token
    if (a := _admin_hash()) and hmac.compare_digest(hashed, a):
        return token
    raise HTTPException(status_code=403, detail="invalid token")


async def require_host_key(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    token = _extract_bearer(request, authorization)
    if not token:
        raise HTTPException(status_code=401, detail="missing bearer token")
    h = _hash_token(token)
    with _db() as conn:
        row = conn.execute(
            """SELECT k.id AS key_id, k.host_id, h.host_uuid, h.hostname, h.enabled
               FROM fleet_api_keys k
               JOIN fleet_hosts h ON h.id = k.host_id
               WHERE k.key_hash = %s AND k.kind = 'host'""",
            (h,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=403, detail="invalid host key")
    if not row["enabled"]:
        raise HTTPException(status_code=403, detail="host disabled")
    return row


# ---------------------------------------------------------------------------
# Metric extraction (used by alert engine)
# ---------------------------------------------------------------------------


def _safe_dig(obj: Any, path: list[str]) -> Any:
    cur = obj
    for key in path:
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return None
    return cur


def extract_metric(payload: dict, metric: str) -> float | None:
    """Resolve a dotted metric name against a sample payload.

    Special composite metrics:
      gpu.temp_max -> max temp across all GPUs
      gpu.util_max -> max utilization
      disk_root.pct -> percent for mount '/'
      zfs.degraded -> count of non-ONLINE pools
      battery.pct_on_battery -> battery.pct if ac_online is false, else None
      host_down -> handled separately (not via this function)
    """
    if metric == "gpu.temp_max":
        gpus = payload.get("gpu") or []
        temps = [g.get("temp") for g in gpus if isinstance(g, dict) and g.get("temp") is not None]
        return float(max(temps)) if temps else None
    if metric == "gpu.util_max":
        gpus = payload.get("gpu") or []
        utils = [g.get("util_pct") for g in gpus if isinstance(g, dict) and g.get("util_pct") is not None]
        return float(max(utils)) if utils else None
    if metric == "disk_root.pct":
        for d in payload.get("disks") or []:
            if isinstance(d, dict) and d.get("mount") == "/":
                v = d.get("pct")
                return float(v) if v is not None else None
        return None
    if metric == "zfs.degraded":
        pools = payload.get("zfs_pools") or []
        return float(sum(1 for p in pools if isinstance(p, dict) and (p.get("state") or "").upper() != "ONLINE"))
    if metric == "battery.pct_on_battery":
        bat = payload.get("battery") or {}
        if isinstance(bat, dict) and bat.get("present") and not bat.get("ac_online"):
            v = bat.get("pct")
            return float(v) if v is not None else None
        return None
    val = _safe_dig(payload, metric.split("."))
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _compare(op: str, value: float, threshold: float) -> bool:
    if op == "gt":
        return value > threshold
    if op == "lt":
        return value < threshold
    if op == "ge":
        return value >= threshold
    if op == "le":
        return value <= threshold
    if op == "eq":
        return value == threshold
    if op == "ne":
        return value != threshold
    return False


# ---------------------------------------------------------------------------
# Latest sample cache (in-process, populated on ingest, used for /hosts list)
# ---------------------------------------------------------------------------

_latest_sample: dict[int, dict[str, Any]] = {}
_latest_lock = asyncio.Lock()


async def _set_latest(host_id: int, payload: dict[str, Any]) -> None:
    async with _latest_lock:
        _latest_sample[host_id] = payload


def latest_sample(host_id: int) -> dict[str, Any] | None:
    return _latest_sample.get(host_id)


# ---------------------------------------------------------------------------
# Background tasks: retention + alert evaluation
# ---------------------------------------------------------------------------


async def retention_loop() -> None:
    while True:
        await asyncio.sleep(3600)
        cutoff = int(time.time()) - SAMPLE_RETENTION_RAW_SECONDS
        try:
            with _db() as conn:
                conn.execute("DELETE FROM fleet_samples WHERE ts < %s", (cutoff,))
                conn.execute(
                    "DELETE FROM fleet_alerts WHERE cleared_at IS NOT NULL AND cleared_at < %s",
                    (int(time.time()) - 30 * 24 * 3600,),
                )
        except Exception as exc:  # noqa: BLE001
            print(f"fleet retention error: {exc}", flush=True)


async def alert_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(evaluate_alerts_once)
        except Exception as exc:  # noqa: BLE001
            print(f"fleet alert eval error: {exc}", flush=True)
        await asyncio.sleep(ALERT_EVAL_INTERVAL_SECONDS)


def evaluate_alerts_once() -> None:
    now = int(time.time())
    with _db() as conn:
        hosts = conn.execute(
            "SELECT id, host_uuid, hostname, display_name, tags, last_seen, enabled FROM fleet_hosts"
        ).fetchall()
        rules = conn.execute(
            "SELECT id, name, host_filter, metric, op, threshold, duration_s, severity, message_tpl FROM fleet_alert_rules WHERE enabled = TRUE"
        ).fetchall()
        active = conn.execute(
            "SELECT id, rule_id, host_id, fired_at, last_value FROM fleet_alerts WHERE cleared_at IS NULL"
        ).fetchall()

        active_by_key = {(a["rule_id"], a["host_id"]): a for a in active}

        for host in hosts:
            if not host["enabled"]:
                continue
            tags = set((host["tags"] or "").split(",")) if host["tags"] else set()
            sample = _latest_sample.get(host["id"]) or {}
            last_seen = host["last_seen"] or 0
            host_down = (last_seen and (now - last_seen) > HOST_DOWN_AFTER_SECONDS) or (not last_seen)

            for rule in rules:
                hf = (rule["host_filter"] or "all").strip()
                if hf != "all":
                    if hf.startswith("tag:"):
                        if hf[4:] not in tags:
                            continue
                    elif hf != host["host_uuid"] and hf != host["hostname"]:
                        continue

                key = (rule["id"], host["id"])
                triggered, value = _rule_triggers(rule, sample, host_down)
                existing = active_by_key.get(key)

                if triggered:
                    if existing:
                        # update last_value
                        if value is not None:
                            conn.execute(
                                "UPDATE fleet_alerts SET last_value = %s WHERE id = %s",
                                (value, existing["id"]),
                            )
                    else:
                        # Only fire if duration condition satisfied. For now we treat
                        # "duration" as a delay: fire immediately if duration_s == 0,
                        # otherwise only if rule.metric has been over threshold for
                        # >= duration_s by looking at last samples. Cheap version:
                        # require sample.ts - rule_first_seen >= duration_s. We
                        # approximate via the freshest sample only — accept eventual
                        # firing after duration_s of ticks. For host_down we already
                        # require HOST_DOWN_AFTER_SECONDS via last_seen.
                        if rule["duration_s"] and rule["metric"] != "host_down":
                            # crude windowed evaluation: scan last duration_s of samples
                            cutoff = now - int(rule["duration_s"])
                            rows = conn.execute(
                                "SELECT payload FROM fleet_samples WHERE host_id = %s AND ts >= %s ORDER BY ts ASC",
                                (host["id"], cutoff),
                            ).fetchall()
                            if not rows:
                                continue
                            all_over = True
                            for r in rows:
                                v = extract_metric(r["payload"], rule["metric"])
                                if v is None or not _compare(rule["op"], v, float(rule["threshold"] or 0)):
                                    all_over = False
                                    break
                            if not all_over:
                                continue
                        message = _format_alert_message(rule, host, value, host_down)
                        conn.execute(
                            """INSERT INTO fleet_alerts (rule_id, host_id, severity, message, fired_at, last_value)
                               VALUES (%s, %s, %s, %s, %s, %s)""",
                            (rule["id"], host["id"], rule["severity"], message, now, value),
                        )
                else:
                    if existing:
                        conn.execute(
                            "UPDATE fleet_alerts SET cleared_at = %s WHERE id = %s",
                            (now, existing["id"]),
                        )


def _rule_triggers(rule: dict, sample: dict, host_down: bool) -> tuple[bool, float | None]:
    if rule["metric"] == "host_down":
        return host_down, 1.0 if host_down else 0.0
    if host_down:
        return False, None
    value = extract_metric(sample, rule["metric"])
    if value is None:
        return False, None
    if rule["threshold"] is None:
        return False, value
    return _compare(rule["op"], value, float(rule["threshold"])), value


def _format_alert_message(rule: dict, host: dict, value: float | None, host_down: bool) -> str:
    name = host["display_name"] or host["hostname"]
    if rule["message_tpl"]:
        try:
            return rule["message_tpl"].format(host=name, value=value, threshold=rule["threshold"])
        except Exception:  # noqa: BLE001
            pass
    if rule["metric"] == "host_down":
        return f"{name} appears down (no samples in >{HOST_DOWN_AFTER_SECONDS}s)"
    vstr = f"{value:.1f}" if isinstance(value, float) else str(value)
    return f"{name}: {rule['metric']} {rule['op']} {rule['threshold']} (current {vstr})"


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/fleet/v1", tags=["fleet"])


@router.get("/status")
async def fleet_status(_: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        hosts = conn.execute("SELECT COUNT(*) AS n FROM fleet_hosts").fetchone()["n"]
        active = conn.execute(
            "SELECT COUNT(*) AS n FROM fleet_hosts WHERE last_seen >= %s",
            (int(time.time()) - HOST_DOWN_AFTER_SECONDS,),
        ).fetchone()["n"]
        alerts = conn.execute("SELECT COUNT(*) AS n FROM fleet_alerts WHERE cleared_at IS NULL").fetchone()["n"]
        samples = conn.execute("SELECT COUNT(*) AS n FROM fleet_samples").fetchone()["n"]
    return {
        "hosts": hosts,
        "active_hosts": active,
        "open_alerts": alerts,
        "samples_stored": samples,
        "retention_raw_days": SAMPLE_RETENTION_RAW_SECONDS // 86400,
        "host_down_after_s": HOST_DOWN_AFTER_SECONDS,
    }


@router.post("/register")
async def fleet_register(payload: dict, _: str = Depends(require_bootstrap)) -> dict[str, Any]:
    host_uuid = (payload.get("host_uuid") or "").strip()
    hostname = (payload.get("hostname") or "").strip()
    if not host_uuid or not hostname:
        raise HTTPException(status_code=400, detail="host_uuid and hostname required")

    now = int(time.time())
    fields = {
        "host_uuid": host_uuid,
        "hostname": hostname,
        "ip": (payload.get("ip") or "").strip() or None,
        "tailscale_ip": (payload.get("tailscale_ip") or "").strip() or None,
        "os": (payload.get("os") or "").strip() or None,
        "distro": (payload.get("distro") or "").strip() or None,
        "kernel": (payload.get("kernel") or "").strip() or None,
        "arch": (payload.get("arch") or "").strip() or None,
        "agent_version": (payload.get("agent_version") or "").strip() or None,
        "tags": (payload.get("tags") or "").strip() or None,
    }

    with _db() as conn:
        existing = conn.execute(
            "SELECT id FROM fleet_hosts WHERE host_uuid = %s", (host_uuid,)
        ).fetchone()
        if existing:
            host_id = existing["id"]
            conn.execute(
                """UPDATE fleet_hosts SET
                       hostname = %s, ip = COALESCE(%s, ip), tailscale_ip = COALESCE(%s, tailscale_ip),
                       os = COALESCE(%s, os), distro = COALESCE(%s, distro), kernel = COALESCE(%s, kernel),
                       arch = COALESCE(%s, arch), agent_version = COALESCE(%s, agent_version),
                       tags = COALESCE(%s, tags), enabled = TRUE
                   WHERE id = %s""",
                (fields["hostname"], fields["ip"], fields["tailscale_ip"], fields["os"], fields["distro"],
                 fields["kernel"], fields["arch"], fields["agent_version"], fields["tags"], host_id),
            )
            existing_key = conn.execute(
                "SELECT id FROM fleet_api_keys WHERE host_id = %s AND kind = 'host'",
                (host_id,),
            ).fetchone()
            if existing_key:
                # Issue a fresh key on re-register to keep flow simple. Old key is revoked.
                conn.execute("DELETE FROM fleet_api_keys WHERE id = %s", (existing_key["id"],))
        else:
            row = conn.execute(
                """INSERT INTO fleet_hosts (host_uuid, hostname, ip, tailscale_ip, os, distro,
                                            kernel, arch, agent_version, tags, registered_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (fields["host_uuid"], fields["hostname"], fields["ip"], fields["tailscale_ip"],
                 fields["os"], fields["distro"], fields["kernel"], fields["arch"],
                 fields["agent_version"], fields["tags"], now),
            ).fetchone()
            host_id = row["id"]

        token = secrets.token_urlsafe(32)
        conn.execute(
            """INSERT INTO fleet_api_keys (host_id, key_hash, kind, label, created_at)
               VALUES (%s, %s, 'host', %s, %s)""",
            (host_id, _hash_token(token), f"agent for {hostname}", now),
        )

    return {
        "host_id": host_id,
        "host_uuid": host_uuid,
        "api_key": token,
        "ingest_path": "/api/fleet/v1/ingest",
    }


@router.post("/ingest")
async def fleet_ingest(payload: dict, host: dict = Depends(require_host_key)) -> dict[str, Any]:
    if "ts" not in payload:
        payload["ts"] = int(time.time())
    ts = int(payload["ts"])
    sample_uuid = (payload.get("host_uuid") or "").strip()
    if sample_uuid and sample_uuid != host["host_uuid"]:
        raise HTTPException(status_code=400, detail="host_uuid mismatch")

    with _db() as conn:
        conn.execute(
            "INSERT INTO fleet_samples (host_id, ts, payload) VALUES (%s, %s, %s)",
            (host["host_id"], ts, Jsonb(payload)),
        )
        conn.execute(
            """UPDATE fleet_hosts SET
                   last_seen = %s,
                   agent_version = COALESCE(%s, agent_version),
                   kernel = COALESCE(%s, kernel),
                   distro = COALESCE(%s, distro),
                   os = COALESCE(%s, os),
                   arch = COALESCE(%s, arch)
               WHERE id = %s""",
            (
                ts,
                (payload.get("agent_version") or None),
                (payload.get("kernel") or None),
                (payload.get("distro") or None),
                (payload.get("os") or None),
                (payload.get("arch") or None),
                host["host_id"],
            ),
        )
        conn.execute("UPDATE fleet_api_keys SET last_used = %s WHERE id = %s", (ts, host["key_id"]))

    await _set_latest(host["host_id"], payload)
    return {"ok": True}


def _host_row_to_dict(row: dict, latest: dict | None) -> dict[str, Any]:
    now = int(time.time())
    last_seen = row["last_seen"] or 0
    up = last_seen and (now - last_seen) <= HOST_DOWN_AFTER_SECONDS
    return {
        "id": row["id"],
        "host_uuid": row["host_uuid"],
        "hostname": row["hostname"],
        "display_name": row["display_name"],
        "ip": row["ip"],
        "tailscale_ip": row["tailscale_ip"],
        "os": row["os"],
        "distro": row["distro"],
        "kernel": row["kernel"],
        "arch": row["arch"],
        "agent_version": row["agent_version"],
        "tags": [t for t in (row["tags"] or "").split(",") if t],
        "location_tag": row["location_tag"],
        "lat": row["lat"],
        "lon": row["lon"],
        "floorplan_id": row["floorplan_id"],
        "floorplan_x": row["floorplan_x"],
        "floorplan_y": row["floorplan_y"],
        "topology_group": row["topology_group"],
        "notes": row["notes"],
        "registered_at": row["registered_at"],
        "last_seen": last_seen or None,
        "up": bool(up),
        "stale_seconds": (now - last_seen) if last_seen else None,
        "enabled": row["enabled"],
        "latest": latest,
    }


@router.get("/hosts")
async def fleet_hosts_list(_: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM fleet_hosts ORDER BY enabled DESC, COALESCE(last_seen, 0) DESC, hostname ASC"
        ).fetchall()
    return {"hosts": [_host_row_to_dict(r, _latest_sample.get(r["id"])) for r in rows]}


@router.get("/hosts/{host_id}")
async def fleet_host_detail(host_id: int, _: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        row = conn.execute("SELECT * FROM fleet_hosts WHERE id = %s", (host_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="host not found")
    return _host_row_to_dict(row, _latest_sample.get(host_id))


@router.patch("/hosts/{host_id}")
async def fleet_host_update(host_id: int, payload: dict, _: str = Depends(require_admin)) -> dict[str, Any]:
    allowed = {"display_name", "tags", "location_tag", "lat", "lon",
               "floorplan_id", "floorplan_x", "floorplan_y", "topology_group",
               "notes", "enabled"}
    sets: list[str] = []
    vals: list[Any] = []
    for key, value in payload.items():
        if key in allowed:
            sets.append(f"{key} = %s")
            vals.append(value)
    if not sets:
        raise HTTPException(status_code=400, detail="no allowed fields provided")
    vals.append(host_id)
    with _db() as conn:
        cur = conn.execute(
            f"UPDATE fleet_hosts SET {', '.join(sets)} WHERE id = %s RETURNING *",
            tuple(vals),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="host not found")
    return _host_row_to_dict(row, _latest_sample.get(host_id))


@router.delete("/hosts/{host_id}")
async def fleet_host_delete(host_id: int, _: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        cur = conn.execute("DELETE FROM fleet_hosts WHERE id = %s", (host_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="host not found")
    _latest_sample.pop(host_id, None)
    return {"ok": True}


_HISTORY_WINDOWS = {
    "hour": {"seconds": 3600, "bucket": 30},
    "day": {"seconds": 24 * 3600, "bucket": 300},
    "week": {"seconds": 7 * 24 * 3600, "bucket": 3600},
}


@router.get("/hosts/{host_id}/history")
async def fleet_host_history(
    host_id: int,
    window: str = "day",
    metric: str | None = None,
    _: str = Depends(require_admin),
) -> dict[str, Any]:
    cfg = _HISTORY_WINDOWS.get(window, _HISTORY_WINDOWS["day"])
    cutoff = int(time.time()) - cfg["seconds"]
    bucket = cfg["bucket"]

    # We can't aggregate jsonb fields generically in SQL; pick a curated set of
    # frequently-needed series and compute them in Python over bucketed samples.
    with _db() as conn:
        rows = conn.execute(
            """SELECT ((ts / %s)::bigint) * %s AS bucket_ts, payload
               FROM fleet_samples
               WHERE host_id = %s AND ts >= %s
               ORDER BY ts ASC""",
            (bucket, bucket, host_id, cutoff),
        ).fetchall()

    buckets: dict[int, dict[str, list[float]]] = {}
    for r in rows:
        b = int(r["bucket_ts"])
        payload = r["payload"]
        target = buckets.setdefault(b, {})
        for key, value in _flatten_metrics(payload):
            target.setdefault(key, []).append(value)

    series_keys: set[str] = set()
    for b in buckets.values():
        series_keys.update(b.keys())
    if metric:
        series_keys = {m for m in series_keys if m == metric or m.startswith(metric + ".")}

    points: list[dict[str, Any]] = []
    for b in sorted(buckets.keys()):
        entry: dict[str, Any] = {"ts": b}
        for key in series_keys:
            vals = buckets[b].get(key) or []
            entry[key] = round(sum(vals) / len(vals), 3) if vals else None
        points.append(entry)

    return {
        "host_id": host_id,
        "window": window,
        "bucket_seconds": bucket,
        "metrics": sorted(series_keys),
        "points": points,
    }


def _flatten_metrics(payload: dict) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    cpu = payload.get("cpu") or {}
    for k in ("pct", "load1", "load5", "load15", "freq_mhz", "temp"):
        v = cpu.get(k)
        if isinstance(v, (int, float)):
            out.append((f"cpu.{k}", float(v)))
    mem = payload.get("mem") or {}
    for k in ("pct", "used", "available", "swap_used"):
        v = mem.get(k)
        if isinstance(v, (int, float)):
            out.append((f"mem.{k}", float(v)))
    for d in payload.get("disks") or []:
        if isinstance(d, dict):
            mount = d.get("mount")
            v = d.get("pct")
            if mount == "/" and isinstance(v, (int, float)):
                out.append(("disk_root.pct", float(v)))
    rx = tx = 0.0
    for n in payload.get("net") or []:
        if isinstance(n, dict):
            rx += float(n.get("rx_bps") or 0)
            tx += float(n.get("tx_bps") or 0)
    out.append(("net.rx_bps", rx))
    out.append(("net.tx_bps", tx))
    gpus = payload.get("gpu") or []
    if gpus:
        temps = [g.get("temp") for g in gpus if isinstance(g, dict) and isinstance(g.get("temp"), (int, float))]
        utils = [g.get("util_pct") for g in gpus if isinstance(g, dict) and isinstance(g.get("util_pct"), (int, float))]
        powers = [g.get("power_w") for g in gpus if isinstance(g, dict) and isinstance(g.get("power_w"), (int, float))]
        if temps:
            out.append(("gpu.temp_max", float(max(temps))))
        if utils:
            out.append(("gpu.util_max", float(max(utils))))
        if powers:
            out.append(("gpu.power_w_total", float(sum(powers))))
    bat = payload.get("battery") or {}
    if isinstance(bat, dict) and bat.get("present"):
        for k in ("pct", "wattage"):
            v = bat.get(k)
            if isinstance(v, (int, float)):
                out.append((f"battery.{k}", float(v)))
    return out


@router.get("/alerts")
async def fleet_alerts_list(
    include_cleared: bool = False,
    limit: int = 100,
    _: str = Depends(require_admin),
) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit or 100), 1000))
    with _db() as conn:
        if include_cleared:
            rows = conn.execute(
                """SELECT a.*, h.hostname, h.display_name, r.name AS rule_name
                   FROM fleet_alerts a
                   LEFT JOIN fleet_hosts h ON h.id = a.host_id
                   LEFT JOIN fleet_alert_rules r ON r.id = a.rule_id
                   ORDER BY a.fired_at DESC LIMIT %s""",
                (safe_limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT a.*, h.hostname, h.display_name, r.name AS rule_name
                   FROM fleet_alerts a
                   LEFT JOIN fleet_hosts h ON h.id = a.host_id
                   LEFT JOIN fleet_alert_rules r ON r.id = a.rule_id
                   WHERE a.cleared_at IS NULL
                   ORDER BY a.fired_at DESC LIMIT %s""",
                (safe_limit,),
            ).fetchall()
    return {"alerts": [dict(r) for r in rows]}


@router.get("/alert-rules")
async def fleet_alert_rules_list(_: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        rows = conn.execute("SELECT * FROM fleet_alert_rules ORDER BY id ASC").fetchall()
    return {"rules": [dict(r) for r in rows]}


@router.post("/alert-rules")
async def fleet_alert_rules_create(payload: dict, _: str = Depends(require_admin)) -> dict[str, Any]:
    required = ("name", "metric", "op")
    for k in required:
        if not payload.get(k):
            raise HTTPException(status_code=400, detail=f"missing {k}")
    now = int(time.time())
    with _db() as conn:
        row = conn.execute(
            """INSERT INTO fleet_alert_rules (name, host_filter, metric, op, threshold, duration_s, severity, message_tpl, enabled, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
            (
                payload["name"], payload.get("host_filter", "all"), payload["metric"], payload["op"],
                payload.get("threshold"), int(payload.get("duration_s") or 60),
                payload.get("severity", "warning"), payload.get("message_tpl"),
                bool(payload.get("enabled", True)), now,
            ),
        ).fetchone()
    return dict(row)


@router.patch("/alert-rules/{rule_id}")
async def fleet_alert_rules_update(rule_id: int, payload: dict, _: str = Depends(require_admin)) -> dict[str, Any]:
    allowed = {"name", "host_filter", "metric", "op", "threshold", "duration_s", "severity", "message_tpl", "enabled"}
    sets: list[str] = []
    vals: list[Any] = []
    for k, v in payload.items():
        if k in allowed:
            sets.append(f"{k} = %s")
            vals.append(v)
    if not sets:
        raise HTTPException(status_code=400, detail="no allowed fields provided")
    vals.append(rule_id)
    with _db() as conn:
        row = conn.execute(
            f"UPDATE fleet_alert_rules SET {', '.join(sets)} WHERE id = %s RETURNING *",
            tuple(vals),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="rule not found")
    return dict(row)


@router.delete("/alert-rules/{rule_id}")
async def fleet_alert_rules_delete(rule_id: int, _: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        cur = conn.execute("DELETE FROM fleet_alert_rules WHERE id = %s", (rule_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="rule not found")
    return {"ok": True}


@router.get("/topology")
async def fleet_topology(_: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        hosts = conn.execute(
            "SELECT id, hostname, display_name, ip, tailscale_ip, topology_group, location_tag, last_seen FROM fleet_hosts WHERE enabled = TRUE"
        ).fetchall()
        edges = conn.execute("SELECT id, a_host_id, b_host_id, kind, label FROM fleet_topology_edges").fetchall()
    now = int(time.time())
    nodes = []
    for h in hosts:
        last_seen = h["last_seen"] or 0
        up = last_seen and (now - last_seen) <= HOST_DOWN_AFTER_SECONDS
        nodes.append(
            {
                "id": h["id"],
                "label": h["display_name"] or h["hostname"],
                "group": h["topology_group"] or "host",
                "ip": h["ip"],
                "tailscale_ip": h["tailscale_ip"],
                "location": h["location_tag"],
                "up": bool(up),
            }
        )
    return {"nodes": nodes, "edges": [dict(e) for e in edges]}


@router.post("/topology/edges")
async def fleet_topology_add_edge(payload: dict, _: str = Depends(require_admin)) -> dict[str, Any]:
    for k in ("a_host_id", "b_host_id", "kind"):
        if not payload.get(k):
            raise HTTPException(status_code=400, detail=f"missing {k}")
    with _db() as conn:
        row = conn.execute(
            """INSERT INTO fleet_topology_edges (a_host_id, b_host_id, kind, label)
               VALUES (%s, %s, %s, %s) RETURNING *""",
            (int(payload["a_host_id"]), int(payload["b_host_id"]),
             payload["kind"], payload.get("label")),
        ).fetchone()
    return dict(row)


@router.delete("/topology/edges/{edge_id}")
async def fleet_topology_del_edge(edge_id: int, _: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        cur = conn.execute("DELETE FROM fleet_topology_edges WHERE id = %s", (edge_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="edge not found")
    return {"ok": True}


@router.get("/floorplans")
async def fleet_floorplans_list(_: str = Depends(require_admin)) -> dict[str, Any]:
    with _db() as conn:
        rows = conn.execute("SELECT * FROM fleet_floorplans ORDER BY id ASC").fetchall()
    return {"floorplans": [dict(r) for r in rows]}


@router.post("/floorplans")
async def fleet_floorplans_create(payload: dict, _: str = Depends(require_admin)) -> dict[str, Any]:
    now = int(time.time())
    name = (payload.get("name") or "").strip() or f"plan-{now}"
    with _db() as conn:
        if payload.get("is_default"):
            conn.execute("UPDATE fleet_floorplans SET is_default = FALSE")
        row = conn.execute(
            """INSERT INTO fleet_floorplans (name, image_path, width, height, is_default, created_at)
               VALUES (%s, %s, %s, %s, %s, %s) RETURNING *""",
            (name, payload.get("image_path"), int(payload.get("width") or 1000),
             int(payload.get("height") or 600), bool(payload.get("is_default", False)), now),
        ).fetchone()
    return dict(row)


@router.get("/export.csv")
async def fleet_export_csv(host_id: int, window: str = "day", _: str = Depends(require_admin)):
    import csv
    cfg = _HISTORY_WINDOWS.get(window, _HISTORY_WINDOWS["day"])
    cutoff = int(time.time()) - cfg["seconds"]
    with _db() as conn:
        rows = conn.execute(
            "SELECT ts, payload FROM fleet_samples WHERE host_id = %s AND ts >= %s ORDER BY ts ASC",
            (host_id, cutoff),
        ).fetchall()
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["ts", "cpu_pct", "mem_pct", "disk_root_pct", "cpu_temp", "gpu_temp_max", "net_rx_bps", "net_tx_bps"])
    for r in rows:
        p = r["payload"]
        cpu = p.get("cpu") or {}
        mem = p.get("mem") or {}
        disk_root = next((d for d in p.get("disks") or [] if isinstance(d, dict) and d.get("mount") == "/"), {})
        gpus = p.get("gpu") or []
        gpu_temp = max((g.get("temp") for g in gpus if isinstance(g, dict) and g.get("temp") is not None), default=None)
        nets = p.get("net") or []
        rx = sum((n.get("rx_bps") or 0) for n in nets if isinstance(n, dict))
        tx = sum((n.get("tx_bps") or 0) for n in nets if isinstance(n, dict))
        writer.writerow([
            r["ts"], cpu.get("pct"), mem.get("pct"), disk_root.get("pct"),
            cpu.get("temp"), gpu_temp, rx, tx,
        ])
    return PlainTextResponse(
        buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="fleet-{host_id}-{window}.csv"'},
    )


# ---------------------------------------------------------------------------
# Web pages (admin-only, auth handled client-side via stored token)
# ---------------------------------------------------------------------------

pages_router = APIRouter(prefix="/fleet", tags=["fleet-pages"])


@pages_router.get("")
async def fleet_index():
    return FileResponse(BASE_DIR / "fleet.html")


@pages_router.get("/")
async def fleet_index_slash():
    return FileResponse(BASE_DIR / "fleet.html")


@pages_router.get("/host/{host_id:int}")
async def fleet_host_page(host_id: int):
    return FileResponse(BASE_DIR / "fleet_host.html")


@pages_router.get("/map")
async def fleet_map_page():
    return FileResponse(BASE_DIR / "fleet_map.html")


@pages_router.get("/alerts")
async def fleet_alerts_page():
    return FileResponse(BASE_DIR / "fleet_alerts.html")


@pages_router.get("/rules")
async def fleet_rules_page():
    return FileResponse(BASE_DIR / "fleet_rules.html")


@pages_router.get("/login")
async def fleet_login_page():
    return FileResponse(BASE_DIR / "fleet_login.html")


_STATIC_ALLOW = {"fleet.css", "fleet.js", "fleet-charts.js"}


@pages_router.get("/static/{filename}")
async def fleet_static(filename: str):
    if filename not in _STATIC_ALLOW:
        raise HTTPException(status_code=404, detail="not found")
    path = BASE_DIR / "fleet_static" / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="not found")
    return FileResponse(path)


_AGENT_BINARIES = {
    "fleet-agent-linux-amd64",
    "fleet-agent-linux-arm64",
    "fleet-agent-linux-386",
    "fleet-agent-linux-arm",
    "fleet-agent-windows-amd64.exe",
    "fleet-agent-darwin-amd64",
    "fleet-agent-darwin-arm64",
}


@router.get("/agent/{name}")
async def fleet_agent_binary(name: str):
    """Public download endpoint for agent binaries.

    Auth-free intentionally: agents must be installable on a fresh host before
    they hold any credentials. The bootstrap token (still required for
    registration) is the auth boundary.
    """
    if name not in _AGENT_BINARIES and name != "install.sh":
        raise HTTPException(status_code=404, detail="unknown asset")
    path = BASE_DIR / "agent_dist" / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="binary not packaged in this image")
    return FileResponse(
        path,
        media_type="application/octet-stream" if name != "install.sh" else "text/x-shellscript",
        filename=name,
    )


# ---------------------------------------------------------------------------
# Bootstrap admin endpoint: lets a logged-in admin issue agent keys without
# the agent itself needing to know the bootstrap token (useful for SSH-push).
# ---------------------------------------------------------------------------


@router.post("/admin/issue-bootstrap")
async def fleet_admin_issue_bootstrap(_: str = Depends(require_admin)) -> dict[str, Any]:
    """Returns a fresh bootstrap-shaped token by minting a single-use key whose
    hash matches `FLEET_BOOTSTRAP_TOKEN_HASH`. Operationally simpler: just
    return the current bootstrap token from the env so admin can paste into
    the agent installer. Returns 404 if not configured."""
    raw = os.getenv("FLEET_BOOTSTRAP_TOKEN", "").strip()
    if not raw:
        raise HTTPException(status_code=404, detail="bootstrap token not exposed (only hash configured)")
    return {"bootstrap_token": raw}
