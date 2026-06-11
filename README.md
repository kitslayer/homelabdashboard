# Homelab Stats Dashboard

A live homelab monitoring stack built with FastAPI and vanilla JS. The public dashboard shows real-time metrics from K3s nodes and Proxmox VE hosts via Server-Sent Events; an auth-gated fleet subsystem ingests metrics from every machine on the network. Live at [stats.milescoviello.com](https://stats.milescoviello.com).

## What it does

- **Live dashboard** (`/`) — SSE-pushed metrics every 5s: per-node K3s CPU/RAM/temp, Proxmox host CPU/RAM/temp, pod count, weather, plus a live incident banner
- **History page** (`/history`) — Postgres-backed charts (day/week/month) with hover tooltips, hall-of-fame stats, visitor geography, request rate, incident timeline, guestbook, changelog, photo carousel
- **Console page** (`/console`) — Live feed of the last 50 public page visits, IPs masked server-side
- **Fleet** (`/fleet`, auth-gated) — central ingest API, admin web UI, and alert engine for every host on the network, with a cross-platform Go agent and a SwiftUI iOS app. See [FLEET.md](FLEET.md)

## Architecture

```text
                   ┌────────── k3s cluster (3 nodes) ──────────┐
Browser ─ ingress ─► DaemonSet: homelab-stats  ◄────► Postgres pod
                   │   app.py   (public, /)            (PVC)   │
agents ── LAN VIP ─►   fleet.py (auth, /fleet)                 │
                   └───────────────────────────────────────────┘
Each pod also reads:
  - host /proc and /sys (hostPID + privileged)
  - Kubernetes API (node/pod status)
  - Proxmox VE API, with SSH hwmon fallback for CPU temps
  - Open-Meteo API (weather, cached server-side)
```

Each DaemonSet pod serves its own node's metrics and exchanges peer metrics over the pod network (`/api/host`, unreachable through any proxy).

## Public-safety design

The public pages never expose real topology:

- Hostnames are pseudonymized (`node-1`, `pve-A`) and internal IPs stripped before anything reaches a browser
- Visitor IPs are masked server-side before they reach any API response or SSE event
- `/api/host` (real hostname, peer exchange) returns 404 for proxied traffic
- Per-IP API rate limiting (`RATE_LIMIT_PER_MIN`, enforced per pod), security headers + CSP, robots.txt
- Weather errors are logged server-side, never echoed to clients

## Incidents

The app detects problems from its own public metrics — K3s node not ready, Proxmox node offline, CPU temp over threshold, database unreachable — with debounced open/close transitions. Open incidents ride the live SSE payload (banner on `/`), and the history page shows a timeline from the `public_incidents` table. Detection state survives DB outages via an in-memory backlog that flushes on recovery. Tune with `INCIDENT_TEMP_C` (default 85) and `INCIDENT_CONFIRM_TICKS` (default 2).

## Public vs private config

This repository intentionally omits live infrastructure details.

- Placeholders like `<LAN_VIP>`, `<NODE_IP>`, `<BUILD_HOST>` mark values that live in private local files or the cluster secret store.
- `k8s.local.yml` and `agent/truenas-compose.local.yml` are git-ignored local copies holding real values.
- Nothing in the repo contains a usable credential; agent binaries under `agent/dist/` and `agent_dist/` are build artifacts (`agent/build.sh`) and are not committed.

## Deployment

Images are built on a separate Docker host and imported into k3s manually (`imagePullPolicy: Never`), then rolled with a versioned tag:

```bash
rsync -az --delete --exclude='.git' --exclude='*.sqlite-bak' --exclude='__pycache__' \
  --exclude='squashfs-root' --exclude='Fleet' --exclude='agent' \
  ./ user@<BUILD_HOST>:/tmp/Stats-build/

ssh user@<BUILD_HOST> 'cd /tmp/Stats-build && docker build -t homelab-stats:vN . \
  && docker save homelab-stats:vN | gzip > /tmp/homelab-stats-vN.tar.gz'

for node in <NODE1_IP> <NODE2_IP> <NODE3_IP>; do
  scp -3 user@<BUILD_HOST>:/tmp/homelab-stats-vN.tar.gz user@$node:/tmp/
  ssh user@$node "sudo k3s ctr images import /tmp/homelab-stats-vN.tar.gz"
done

ssh user@<NODE1_IP> "sudo kubectl set image ds/homelab-stats homelab-stats=homelab-stats:vN \
  && sudo kubectl rollout status ds/homelab-stats"
```

## Secrets

Two Kubernetes secrets; no credentials in the repo:

| Secret | Keys |
|--------|------|
| `postgres-creds` | `DATABASE_URL` (mounted via envFrom) |
| `homelab-stats-secrets` | `pve-token`, `pve-ssh-pass`, `weather-lat`/`weather-lon` (or `weather-proxy-url`), `guestbook-mod-token`, `fleet-admin-token-hash`, `fleet-bootstrap-token-hash`, `fleet-bootstrap-token` |

Token generation and rotation commands are in [FLEET.md](FLEET.md).

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | Postgres connection string |
| `PVE_IPS` / `PVE_TOKEN` / `PVE_TIMEOUT` | unset | Proxmox API polling |
| `LIVE_INTERVAL_SECONDS` | `5` | Metric collection cadence |
| `SNAPSHOT_INTERVAL_SECONDS` | `60` | DB snapshot cadence |
| `RATE_LIMIT_PER_MIN` | `120` | Per-IP `/api/*` request budget, per pod |
| `INCIDENT_TEMP_C` | `85` | Temperature incident threshold (°C) |
| `INCIDENT_CONFIRM_TICKS` | `2` | Consecutive ticks before an incident opens/closes |
| `GUESTBOOK_RATE_LIMIT_SECONDS` | `300` | Cooldown between guestbook submissions |
| `WEATHER_LABEL` / `WEATHER_CACHE_SECONDS` | — | Weather card tuning |
| `K8S_NODE_NAME` / `POD_NAMESPACE` | downward API | Pod identity for peer discovery |

## Key gotchas

- `imagePullPolicy: Never` — import the image to **all** nodes before rolling, or pods fail wherever it's missing
- The pod runs privileged with `hostPID: true` to read host `/proc` and `/sys`
- Proxmox temps fall back to SSH + hwmon because the PVE 9.x API dropped `thermalstate`
- No GZip middleware — it breaks SSE (browser EventSource can't decompress)
- Request events are buffered in memory and batch-inserted every 500ms
- `k8s.yml` is a historical example; the live DaemonSet is managed with `kubectl set image` / `set env`

## File overview

| File | Purpose |
|------|---------|
| `app.py` | Public dashboard — metrics, SSE, incidents, guestbook, hardening |
| `fleet.py` | Fleet ingest API, alert engine, admin UI routes |
| `index.html` / `history.html` / `console.html` | Public pages |
| `fleet*.html`, `fleet_static/` | Fleet admin UI |
| `agent/` | Go agent source, cross-compile + install scripts, TrueNAS compose |
| `Fleet/` | SwiftUI iOS app (xtool-based SPM project) |
| `FLEET.md` | Fleet architecture and operations runbook |
| `Dockerfile` | python:3.12-slim + sshpass/openssh-client |
| `requirements.txt` | Python dependencies |
