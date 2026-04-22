# Homelab Stats Dashboard

A live homelab monitoring dashboard built with FastAPI and vanilla JS. It displays real-time metrics from K3s and Proxmox over Server-Sent Events, plus historical charts, weather, and a moderated guestbook.

## What it does

- **Live dashboard** — SSE-pushed metrics every 5s: K3s node CPU, RAM, temp, pod count, and optional Proxmox/weather data
- **History page** — SQLite-backed charts, hall-of-fame stats, visitor geography, request rate, guestbook, changelog, and photo carousel
- **Console page** — Live feed of the last 50 public page visits

## Public vs private config

This repository intentionally omits live infrastructure details.

- Keep real hostnames, IPs, storage paths, usernames, and coordinates in private local files such as `k8s.local.yml` or in your cluster secret store.
- `k8s.yml` is a public example manifest with documentation-only values.
- `k8s.local.yml` is ignored by git and can keep your current working deployment settings on this machine.

## Architecture

```text
Browser -> Ingress or NodePort -> homelab-stats pod (FastAPI/uvicorn)
                                         |
                                         +-> SQLite database on persistent storage
                                         +-> Kubernetes API
                                         +-> Proxmox VE API
                                         +-> Optional SSH temperature probes
                                         +-> Open-Meteo API
```

The pod also reads host `/proc` and `/sys` when deployed with `hostPID: true` and a privileged security context.

## Deployment

The repo does not ship your live infrastructure values. Put those in `k8s.local.yml`, then deploy that file instead of the public example.

Example workflow:

```bash
export BUILD_HOST='deploy@build-host.example.internal'
export CONTROL_NODE='ops@k3s-control.example.internal'
export K3S_NODES='ops@k3s-1.example.internal ops@k3s-2.example.internal ops@k3s-3.example.internal'

# 1. Copy app files to the build host
scp app.py console.html history.html index.html requirements.txt Dockerfile "$BUILD_HOST:/tmp/homelab-stats/"

# 2. Build the image
ssh "$BUILD_HOST" "docker build --no-cache -t homelab-stats:latest /tmp/homelab-stats"

# 3. Export the image
ssh "$BUILD_HOST" "docker save homelab-stats:latest | gzip > /tmp/homelab-stats.tar.gz"

# 4. Import the image to each K3s node
for node in $K3S_NODES; do
  scp "$BUILD_HOST:/tmp/homelab-stats.tar.gz" "$node:/tmp/homelab-stats.tar.gz"
  ssh "$node" "sudo k3s ctr images import /tmp/homelab-stats.tar.gz"
done

# 5. Apply your private manifest
scp k8s.local.yml "$CONTROL_NODE:/tmp/homelab-stats-k8s.yml"
ssh "$CONTROL_NODE" "sudo kubectl apply -f /tmp/homelab-stats-k8s.yml"

# 6. Restart after code-only changes
ssh "$CONTROL_NODE" "sudo kubectl rollout restart deployment/homelab-stats"
```

## Secrets

Create the Kubernetes secret before first deploy:

```bash
ssh "$CONTROL_NODE" "sudo kubectl create secret generic homelab-stats-secrets \
  --from-literal=pve-token='USER@pam!tokenid=TOKEN_VALUE' \
  --from-literal=pve-ssh-pass='YOUR_PVE_ROOT_PASSWORD' \
  --from-literal=weather-lat='YOUR_WEATHER_LAT' \
  --from-literal=weather-lon='YOUR_WEATHER_LON' \
  --from-literal=guestbook-mod-token='YOUR_MOD_TOKEN'"
```

| Secret key | Purpose |
|------------|---------|
| `pve-token` | Proxmox API token (`USER@pam!id=secret` format) |
| `pve-ssh-pass` | Root SSH password for PVE nodes when SSH temp probing is enabled |
| `weather-lat` / `weather-lon` | Coordinates for Open-Meteo weather fetches |
| `guestbook-mod-token` | Token for approving or rejecting guestbook entries |

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STATS_DB_PATH` | `/data/stats.db` | SQLite DB path |
| `PVE_IPS` | unset | Comma-separated Proxmox host IPs |
| `PVE_TIMEOUT` | `4` | PVE API request timeout in seconds |
| `WEATHER_LABEL` | `Outdoor weather` | Label shown on the dashboard |
| `LIVE_INTERVAL_SECONDS` | `5` | Metrics collection interval |
| `SNAPSHOT_INTERVAL_SECONDS` | `60` | DB snapshot interval |
| `GUESTBOOK_RATE_LIMIT_SECONDS` | `300` | Cooldown between guestbook submissions |
| `K8S_NODE_NAME` | downward API | Node name for the running pod |

## Persistence

The app supports either a persistent volume or a local file path.

- The public `k8s.yml` uses example NFS values from the documentation-only `192.0.2.0/24` range.
- Replace those values in `k8s.local.yml` before deploying.
- If the primary DB path is unavailable at runtime, the app falls back to `/tmp/stats_fallback.db` and retries the primary path every 30 seconds.

## Security notes

- Prefer ingress, VPN, or other access control instead of exposing a raw public NodePort.
- The pod runs privileged with `hostPID: true`; treat it as a trusted internal deployment.
- Guestbook moderation expects the token in the `x-mod-token` header, not in a query string.

## File overview

| File | Purpose |
|------|---------|
| `app.py` | FastAPI backend for metrics collection, SSE, SQLite, weather, and guestbook |
| `index.html` | Live dashboard |
| `history.html` | Historical data, charts, and guestbook |
| `console.html` | Live request feed |
| `Dockerfile` | Python 3.12-slim image with SSH tooling |
| `requirements.txt` | Python dependencies |
| `k8s.yml` | Public example K3s manifest |
