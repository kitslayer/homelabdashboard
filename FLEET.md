# Fleet — homelab metrics, alerts, and iPhone app

Built on top of the existing `homelab-stats` deployment so the public
dashboard at `stats.milescoviello.com` keeps working untouched (since 2026-06 the bare domain serves the portfolio site). The fleet
subsystem lives under `/api/fleet/v1/*` (auth-gated) and `/fleet/*` (admin
web UI), sharing the same FastAPI process and Postgres database.

Status (2026-05-12): backend deployed, 8 hosts pushing samples, web UI
live, iOS app source ready. Pending: Surface and T30 were offline; TrueNAS
needs the Docker compose route (yml provided).

---

## What you get

- A central ingest API that any number of hosts can push 30-second
  metric snapshots to, over LAN or the public domain.
- A web admin UI (`/fleet`) with host grid, per-host history charts,
  three-mode map (network topology / geographic / floorplan), an alerts
  page, and an alert-rule editor.
- A Go agent that runs as a systemd unit, OpenRC service, Windows
  service, or Docker container — same binary, same protocol, on Linux
  amd64/arm64/386/arm, Windows amd64, macOS amd64/arm64.
- A SwiftUI iPhone app (built with `xtool`, no Mac required) that
  mirrors the web UI on the phone, with local push notifications for
  alerts.
- A server-side alert engine that evaluates every rule against every
  host every ~30 s and writes to `fleet_alerts` (read by the iOS app and
  the web UI).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                       k3s cluster (3 nodes)                         │
│                                                                     │
│  ┌──── DaemonSet: homelab-stats:v2 ────┐    ┌── Postgres pod ──┐   │
│  │  app.py  (public dashboard, /)     │◄──►│  postgres-16     │   │
│  │  fleet.py (this system, /fleet)    │    │  PVC on NFS      │   │
│  │  fleet_*.html, fleet_static/       │    └──────────────────┘   │
│  │  agent_dist/ (binaries + tar)      │                            │
│  └─────────────────────────────────────┘                            │
│              ▲                                                      │
│              │ via Service homelab-stats-lb                         │
└──────────────┼──────────────────────────────────────────────────────┘
               │
        MetalLB │  <LAN_VIP>   ◄──── agents (LAN)
               │                   ◄──── iPhone app (LAN / Tailscale)
               │
        NPM    │  https://stats.milescoviello.com  (public fallback)
               │
        ──────┴───── traefik (<INGRESS_IP>)  ◄── public dashboard
```

**Agents** push to whichever ingress works first. Each agent caches the
last-working server URL and prefers it next time. State (host_uuid,
api_key) lives in `/var/lib/fleet-agent/state.json` on the host.

**Auth**: three secrets share the same K8s `homelab-stats-secrets`:

| Key in secret                  | Used by                       |
|---------------------------------|-------------------------------|
| `fleet-admin-token-hash`        | server: validates admin tokens|
| `fleet-bootstrap-token-hash`    | server: validates bootstrap   |
| `fleet-bootstrap-token`         | agents: register once         |

The admin token is never on disk anywhere except the K8s secret (hash
only) and your password manager. Each agent registers with the bootstrap
token and is given its own long-lived API key.

---

## Repo layout (under `/home/miles/Stats/`)

```
app.py                  existing public-dashboard FastAPI app
fleet.py                NEW — fleet router, schema, alert engine
fleet.html              admin host grid
fleet_host.html         per-host detail + history charts
fleet_map.html          topology / geo / floorplan toggle
fleet_alerts.html       alerts table
fleet_rules.html        alert-rule editor
fleet_login.html        token entry
fleet_static/
  fleet.css             dark theme, panels, pills
  fleet.js              shared client (auth, fetch, fmt)
Dockerfile              homelab-stats container, now copies fleet_*
k8s.local.yml           DaemonSet manifest (older; live state is patched)
agent/
  go.mod
  main.go               register + push loop + state persistence
  collect.go            common types
  collect_linux.go      /proc, /sys, nvidia-smi, zpool, smartctl, ...
  collect_windows.go    powershell + nvidia-smi
  collect_darwin.go     sysctl + pmset
  build.sh              cross-compile all 7 targets
  install.sh            systemd + openrc installer
  Dockerfile            container image for TrueNAS
  truenas-compose.yml   ready-to-paste TrueNAS Custom App
  TRUENAS.md            TrueNAS-specific walkthrough
  dist/                 cross-compiled binaries (gitignored)
  fleet-agent-0.1.0.tar Docker image for TrueNAS (gitignored)
agent_dist/             same binaries, but baked into the container
                        image so they're downloadable from
                        /api/fleet/v1/agent/<name>
Fleet/                  SwiftUI iOS app (xtool-based SPM project)
  Package.swift
  xtool.yml             bundleID: com.milescoviello.Fleet
  README.md             user-facing build instructions
  Sources/Fleet/
    FleetApp.swift      @main, tab view, root
    Theme.swift         color palette + panel modifier
    Models.swift        Host / Sample / Alert / History codables
    Session.swift       token + server URLs (UserDefaults)
    Network.swift       failover client (last-working → primary → fallback)
    FleetStore.swift    polling loop, local-notification dispatch
    LoginView.swift     admin token entry
    DashboardView.swift host grid + sort + search
    HostDetailView.swift live + history (Swift Charts)
    MapView.swift       3-mode map
    AlertsView.swift    open + cleared
    SettingsView.swift  servers, status, sign out
```

---

## Endpoints (admin-bearer-token, except register/ingest/agent)

| Method | Path                                            | Auth          | Notes                                |
|--------|-------------------------------------------------|---------------|--------------------------------------|
| GET    | `/api/fleet/v1/status`                          | admin         | basic counts                         |
| POST   | `/api/fleet/v1/register`                        | bootstrap     | issues per-host api_key              |
| POST   | `/api/fleet/v1/ingest`                          | host key      | agent push                           |
| GET    | `/api/fleet/v1/hosts`                           | admin         | list hosts with latest sample        |
| GET    | `/api/fleet/v1/hosts/{id}`                      | admin         | single host                          |
| PATCH  | `/api/fleet/v1/hosts/{id}`                      | admin         | edit display_name, tags, lat/lon, ...|
| DELETE | `/api/fleet/v1/hosts/{id}`                      | admin         | remove host + samples                |
| GET    | `/api/fleet/v1/hosts/{id}/history?window=...`   | admin         | bucketed metrics; window=hour,day,week |
| GET    | `/api/fleet/v1/alerts?include_cleared=...`      | admin         | alert feed                           |
| GET    | `/api/fleet/v1/alert-rules`                     | admin         | list rules                           |
| POST   | `/api/fleet/v1/alert-rules`                     | admin         | new rule                             |
| PATCH  | `/api/fleet/v1/alert-rules/{id}`                | admin         | update                               |
| DELETE | `/api/fleet/v1/alert-rules/{id}`                | admin         | delete                               |
| GET    | `/api/fleet/v1/topology`                        | admin         | nodes + edges                        |
| POST   | `/api/fleet/v1/topology/edges`                  | admin         | add edge                             |
| DELETE | `/api/fleet/v1/topology/edges/{id}`             | admin         | remove edge                          |
| GET    | `/api/fleet/v1/floorplans` / POST               | admin         | floorplan metadata                   |
| GET    | `/api/fleet/v1/export.csv?host_id=&window=`     | admin         | CSV export                           |
| GET    | `/api/fleet/v1/agent/{name}`                    | **public**    | download agent binary / install.sh   |

Static + page routes (admin token via localStorage):

| Path                | What                              |
|---------------------|-----------------------------------|
| `/fleet/login`      | token entry                       |
| `/fleet`            | host grid                         |
| `/fleet/host/{id}`  | per-host detail                   |
| `/fleet/map`        | three-mode map                    |
| `/fleet/alerts`     | alerts                            |
| `/fleet/rules`      | alert-rule editor                 |
| `/fleet/static/...` | css/js                            |

---

## Reaching the service

- **LAN VIP**: `http://<LAN_VIP>` — MetalLB on Service `homelab-stats-lb`. This is the canonical address for both agents and the iOS app inside the house.
- **NodePort**: `http://<NODE_IP>:30081` (any node) — works without going through MetalLB, single-node fallback.
- **Public**: `https://stats.milescoviello.com` — only the public dashboard is exposed at this URL, but `/fleet/*` and `/api/fleet/v1/*` also work behind it (auth is your bearer token; no Authelia layer in front of the API yet).

---

## Default alert rules

Seeded on first start of `fleet.py`. Edit them at `/fleet/rules`.

| Rule              | Metric                  | Op  | Threshold | Duration | Severity   |
|-------------------|-------------------------|-----|-----------|----------|------------|
| Host down         | host_down               | gt  | 0         | 0        | critical   |
| CPU > 90%         | cpu.pct                 | gt  | 90        | 5 min    | warning    |
| Memory > 90%      | mem.pct                 | gt  | 90        | 5 min    | warning    |
| Disk > 85% on /   | disk_root.pct           | gt  | 85        | 5 min    | warning    |
| CPU temp > 90 C   | cpu.temp                | gt  | 90        | 1 min    | critical   |
| GPU temp > 90 C   | gpu.temp_max            | gt  | 90        | 1 min    | warning    |
| ZFS degraded      | zfs.degraded            | gt  | 0         | 1 min    | critical   |
| Battery low       | battery.pct_on_battery  | lt  | 15        | 1 min    | warning    |

You can add more via the UI; the metric path is dotted dig into the
sample JSON (`cpu.pct`, `mem.pct`, `cpu.temp`, …) plus a few computed
ones (`gpu.temp_max`, `gpu.util_max`, `disk_root.pct`, `zfs.degraded`,
`battery.pct_on_battery`, `host_down`).

---

## Installing the agent on a new host

### Linux (systemd or OpenRC)

From the laptop:

```bash
scp /home/miles/Stats/agent/install.sh \
    /home/miles/Stats/agent/dist/fleet-agent-linux-amd64 \
    user@host:/tmp/

ssh user@host 'sudo /tmp/install.sh \
    --server http://<LAN_VIP> \
    --fallback https://stats.milescoviello.com \
    --bootstrap "<BOOTSTRAP_TOKEN>" \
    --tags "tag1,tag2" \
    --display-name "Friendly Name" \
    --bin-path /tmp/fleet-agent-linux-amd64'
```

Arch-specific binaries are in `agent/dist/`:
- `fleet-agent-linux-amd64`  – Intel/AMD 64-bit
- `fleet-agent-linux-arm64`  – aarch64 (RPi 4/5 etc)
- `fleet-agent-linux-386`    – T30 ThinkPad and other i686
- `fleet-agent-linux-arm`    – armv7

The installer auto-detects systemd vs OpenRC. After a successful
registration it strips the bootstrap token from `/etc/fleet-agent.env`.

### Windows

```powershell
# copy fleet-agent-windows-amd64.exe to C:\Program Files\FleetAgent\
# create a Windows Service:
New-Service -Name FleetAgent `
            -BinaryPathName 'C:\Program Files\FleetAgent\fleet-agent.exe' `
            -StartupType Automatic
# set the env vars (FLEET_SERVER, FLEET_BOOTSTRAP_TOKEN, etc.) via
# `setx /M FLEET_SERVER http://<LAN_VIP>`
Start-Service FleetAgent
```

### TrueNAS

Read-only `/usr/local/bin`, so use the Docker route:

1. `scp /home/miles/Stats/agent/fleet-agent-0.1.0.tar truenas_admin@<TRUENAS_HOST>:/mnt/ssd-pool/apps/fleet-agent/image.tar`
2. In the TrueNAS shell: `docker load -i /mnt/ssd-pool/apps/fleet-agent/image.tar`
3. In the web UI: Apps → Discover → Custom App → paste `agent/truenas-compose.yml`.

Full walkthrough: `agent/TRUENAS.md`.

### macOS

Same `install.sh` but with `--bin-path .../dist/fleet-agent-darwin-arm64`
(or `-amd64` for Intel Macs). It will write a `launchd` plist instead of
a systemd unit (TODO — not implemented yet; for now, run the binary
manually under launchd or via a brew service stub).

---

## Building the iPhone app

Pre-installed on the laptop:
- Swift 6.3.1 via `swiftly` at `~/.local/share/swiftly/`
- `xtool` 1.16.1 at `/usr/local/bin/xtool`
- `libxml2.so.2` compat symlink at `~/.local/lib/swift-compat/`
  (Gentoo ships libxml2.so.16; Swift needs the older soname)

You still need to do once (Apple-gated):

1. **Download Xcode 26.0.1 .xip** from
   <https://download.developer.apple.com/Developer_Tools/Xcode_26.0.1/Xcode_26.0.1_Apple_silicon.xip>
   in a browser. Apple ID auth required.
2. **Run xtool setup** in a normal terminal (not the Claude shell, which
   has epoll restrictions that crash xtool):
   ```
   cd /home/miles/Stats/Fleet
   . ~/.local/share/swiftly/env.sh
   xtool setup
   # choose option 1 (password auth, free Apple ID)
   # use your throwaway Apple ID
   # point at the Xcode.xip you downloaded
   ```
   This extracts the iOS SDK into `swift sdk list` (takes ~10 minutes).
3. **Plug in your iPhone** over USB. Install `usbmuxd` + `libimobiledevice`:
   ```
   sudo emerge net-libs/libimobiledevice app-pda/usbmuxd
   sudo rc-service usbmuxd start
   ```
4. **Build + install**:
   ```
   cd /home/miles/Stats/Fleet
   xtool dev
   ```
   This builds the SPM project for `arm64-apple-ios`, signs the IPA with
   your dev cert, and installs to the connected device. Re-run `xtool
   dev` after edits.

Free Apple Developer cert means the app expires after 7 days. Run `xtool
dev` again any time to refresh.

The app stores its admin token + server URLs in UserDefaults on the
device. Server defaults match `<LAN_VIP>` and `stats.milescoviello.com`.

---

## Deploying TrueNAS-specific compose

See `agent/TRUENAS.md` for the canonical steps. Fill in
`FLEET_BOOTSTRAP_TOKEN` in `agent/truenas-compose.yml` from your
password manager (a pre-filled copy lives untracked at
`agent/truenas-compose.local.yml`), and update the pool path if you
aren't on `ssd-pool`.

---

## Operational reference

### Generate fresh tokens

```bash
python3 -c "
import secrets, hashlib
t = secrets.token_urlsafe(32)
print('ADMIN_TOKEN  ', t)
print('ADMIN_HASH   ', hashlib.sha256(t.encode()).hexdigest())
b = secrets.token_urlsafe(24)
print('BOOTSTRAP    ', b)
print('BOOTSTRAP_H  ', hashlib.sha256(b.encode()).hexdigest())
"
```

### Rotate the admin or bootstrap token

```bash
# update the secret
ssh user@<NODE_IP> "sudo kubectl patch secret homelab-stats-secrets \
    --type=merge -p '{\"stringData\":{\"fleet-admin-token-hash\":\"<NEW_HASH>\"}}'"
# restart the DaemonSet to reread the secret
ssh user@<NODE_IP> "sudo kubectl rollout restart ds/homelab-stats"
```

Bootstrap is the same with key `fleet-bootstrap-token-hash` (and
`fleet-bootstrap-token` if you want the plain token reachable via
`/api/fleet/v1/admin/issue-bootstrap`).

### Rebuild the container after editing fleet.py / pages / agent

```bash
# 1. push source to .195
rsync -az --delete --exclude='.git' --exclude='*.sqlite-bak' --exclude='__pycache__' \
    /home/miles/Stats/ user@<BUILD_HOST>:/tmp/Stats-build/

# 2. build the image on .195
ssh user@<BUILD_HOST> \
    'cd /tmp/Stats-build && sudo docker build -t homelab-stats:v3 .'

# 3. save + distribute
ssh user@<BUILD_HOST> 'sudo docker save -o /tmp/homelab-stats-v3.tar homelab-stats:v3 && sudo chmod 644 /tmp/homelab-stats-v3.tar'
scp user@<BUILD_HOST>:/tmp/homelab-stats-v3.tar /tmp/
for n in <NODE1_IP> <NODE2_IP> <NODE3_IP>; do
    scp /tmp/homelab-stats-v3.tar user@$n:/tmp/
    ssh user@$n "sudo k3s ctr images import /tmp/homelab-stats-v3.tar"
done

# 4. point the DaemonSet at the new tag and roll
ssh user@<NODE_IP> "sudo kubectl set image ds/homelab-stats homelab-stats=homelab-stats:v3"
ssh user@<NODE_IP> "sudo kubectl rollout status ds/homelab-stats"
```

### Add a new alert rule (without using the UI)

```bash
curl -X POST -H "Authorization: Bearer $FLEET_ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     http://<LAN_VIP>/api/fleet/v1/alert-rules -d '{
       "name": "Loadavg > 8 on k3s",
       "host_filter": "tag:k3s",
       "metric": "cpu.load1",
       "op": "gt",
       "threshold": 8,
       "duration_s": 120,
       "severity": "warning"
     }'
```

`host_filter` values: `all`, `tag:<tag>`, or a literal `host_uuid` /
`hostname`.

### Retention

Raw samples drop after 7 days (`FLEET_SAMPLE_RETENTION_RAW` env). The
retention loop runs hourly. There are no rollups yet — at ~30 hosts ×
30 s push, you generate ~83 k rows/day, ~600 k rows/week — Postgres
handles this trivially for now. Add `pg_cron` rollups when it starts to
hurt.

---

## Currently registered hosts (as of build)

| ID | Display name     | IP            | OS / Init      |
|----|------------------|---------------|----------------|
| 1  | gentoo (laptop)  | LAN           | Gentoo / OpenRC|
| 2  | ubuntudockerultra| LAN           | Ubuntu / systemd|
| 3  | k3s-1            | LAN           | systemd        |
| 4  | k3s-2            | LAN           | systemd        |
| 5  | k3s-3            | LAN           | systemd        |
| 6  | pveA4            | LAN           | Debian / systemd|
| 7  | pvei7            | 100.x.y.z (TS)| Debian / systemd|
| 8  | ubuntu4070ti     | LAN           | Ubuntu / systemd|

Not yet registered:

- **TrueNAS** — needs the Docker compose route (yml ready).
- **Surface Pro** — was offline at install time. When it's online,
  run the linux-amd64 installer (CachyOS is systemd).
- **T30 ThinkPad** — was offline at install time. Use the 386
  binary: `--bin-path .../dist/fleet-agent-linux-386`.

---

## What is NOT done yet

- **No public-Internet auth wrapper** on `/api/fleet/v1/*`. It's
  bearer-token-only. If you don't want random people probing it from the
  Internet, put Authelia forward-auth in front of `*.milescoviello.com`
  for `/fleet/*` (the iOS app and agents still need a non-Authelia path,
  so leave `/api/fleet/v1/ingest` open).
- **APNs** (real push notifications). The iOS app uses local
  notifications during a background-refresh poll. Adding APNs requires
  a paid Apple Developer Program membership.
- **Control / actions from the iOS app** (reboot, restart service,
  exec). Schema slot is reserved in `fleet.py` but no endpoints
  implemented. Plan: a `fleet_commands` table + `POST
  /api/fleet/v1/hosts/{id}/cmd` writes a row; agent polls a
  `/cmd-queue` endpoint and acks. Don't ship without a per-action
  confirmation in the UI.
- **Sample rollups** (5-min / 1-hour). Not needed at current data
  volume; add `pg_cron` or a background worker when historical-window
  reads slow down.
- **macOS launchd install** — `install.sh` only handles systemd /
  OpenRC. The binary is built; just need the plist.
- **Floorplan image upload** — schema exists in `fleet_floorplans`, the
  positioning UI works (drag pins, persists via PATCH), but you can't
  upload a background image through the UI yet. Drop a PNG into the
  container's `/data/floorplans/` and set `image_path` via the API.

---

## References

- Public dashboard: `https://stats.milescoviello.com` — unchanged by the fleet subsystem.
- Admin and bootstrap tokens live in the password manager and the
  `homelab-stats-secrets` K8s secret (hashes only server-side). Nothing
  in this repo contains a usable token.
