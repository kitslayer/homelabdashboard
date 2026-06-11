#!/usr/bin/env bash
# install.sh — fleet-agent installer for Linux hosts.
#
# Usage (run on the target host):
#   curl -sL http://<SERVER>/agent/install.sh | sudo bash -s -- \
#         --server http://<SERVER>/ \
#         --fallback https://milescoviello.com \
#         --bootstrap "$BOOTSTRAP_TOKEN" \
#         --tags "laptop,gentoo"
#
# Or run locally after scp:
#   sudo ./install.sh --server http://<SERVER>/ --bootstrap "$BOOTSTRAP"
#
# The script:
#   1. Detects arch + libc
#   2. Downloads or installs the matching binary to /usr/local/bin/fleet-agent
#   3. Writes /etc/fleet-agent.env with the chosen flags
#   4. Installs a systemd unit OR OpenRC init script and enables it
set -euo pipefail

SERVER=""
FALLBACK=""
BOOTSTRAP=""
TAGS=""
INTERVAL="30"
TAILSCALE_IP=""
DISPLAY_NAME=""
BIN_URL=""
BIN_PATH=""
DEFAULT_BASE_URL=""

usage() {
    cat <<EOF
fleet-agent installer

Required:
  --server URL            primary ingest server
  --bootstrap TOKEN       one-shot registration token (only on first install)

Optional:
  --fallback URL          fallback ingest server
  --tags csv,tags         comma-separated tags on this host
  --interval seconds      push interval (default 30, min 5)
  --tailscale-ip IP       advertised tailscale IP
  --display-name NAME     pretty name shown in the dashboard
  --bin-path PATH         local fleet-agent binary to install
  --bin-url URL           HTTP URL to fetch fleet-agent binary
  --base-url URL          if set, derive bin-url from <base>/dist/<arch>/fleet-agent
EOF
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --server) SERVER="$2"; shift 2 ;;
        --fallback) FALLBACK="$2"; shift 2 ;;
        --bootstrap) BOOTSTRAP="$2"; shift 2 ;;
        --tags) TAGS="$2"; shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --tailscale-ip) TAILSCALE_IP="$2"; shift 2 ;;
        --display-name) DISPLAY_NAME="$2"; shift 2 ;;
        --bin-path) BIN_PATH="$2"; shift 2 ;;
        --bin-url) BIN_URL="$2"; shift 2 ;;
        --base-url) DEFAULT_BASE_URL="$2"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "unknown option: $1" >&2; usage ;;
    esac
done

[ -z "$SERVER" ] && { echo "--server is required" >&2; exit 1; }

if [ "$(id -u)" -ne 0 ]; then
    echo "must run as root (sudo)" >&2
    exit 1
fi

# Detect arch
ARCH_RAW="$(uname -m)"
case "$ARCH_RAW" in
    x86_64|amd64) GOARCH="amd64" ;;
    aarch64|arm64) GOARCH="arm64" ;;
    i386|i686) GOARCH="386" ;;
    armv7l|armv6l) GOARCH="arm" ;;
    *) echo "unsupported arch $ARCH_RAW" >&2; exit 1 ;;
esac

DEST="/usr/local/bin/fleet-agent"

if [ -n "$BIN_PATH" ]; then
    install -m 0755 "$BIN_PATH" "$DEST"
elif [ -n "$BIN_URL" ]; then
    echo "Downloading from $BIN_URL"
    curl -fsSL "$BIN_URL" -o "$DEST"
    chmod +x "$DEST"
elif [ -n "$DEFAULT_BASE_URL" ]; then
    URL="${DEFAULT_BASE_URL%/}/fleet-agent-linux-${GOARCH}"
    echo "Downloading from $URL"
    curl -fsSL "$URL" -o "$DEST"
    chmod +x "$DEST"
elif [ -x "$DEST" ]; then
    echo "Reusing existing $DEST (no --bin-path/--bin-url provided)"
else
    echo "no binary source provided (--bin-path, --bin-url, or --base-url required)" >&2
    exit 1
fi

# Verify the binary at least runs.
"$DEST" --version || { echo "downloaded binary failed --version" >&2; exit 1; }

# Write env file with shell-quoted values so OpenRC's `set -a; .` works as
# well as systemd's EnvironmentFile (both accept unquoted KEY=VALUE, but
# systemd doesn't run shell so quotes around values containing spaces are
# safe in either init).
ENV_FILE="/etc/fleet-agent.env"
sq() { printf '%s' "$1" | sed "s/'/'\\\\''/g; s/^/'/; s/\$/'/"; }
{
    echo "FLEET_SERVER=$(sq "$SERVER")"
    [ -n "$FALLBACK" ] && echo "FLEET_SERVER_FALLBACK=$(sq "$FALLBACK")"
    [ -n "$TAGS" ] && echo "FLEET_TAGS=$(sq "$TAGS")"
    [ -n "$INTERVAL" ] && echo "FLEET_INTERVAL=$(sq "$INTERVAL")"
    [ -n "$TAILSCALE_IP" ] && echo "FLEET_TAILSCALE_IP=$(sq "$TAILSCALE_IP")"
    [ -n "$DISPLAY_NAME" ] && echo "FLEET_DISPLAY_NAME=$(sq "$DISPLAY_NAME")"
    [ -n "$BOOTSTRAP" ] && echo "FLEET_BOOTSTRAP_TOKEN=$(sq "$BOOTSTRAP")"
} > "$ENV_FILE"
chmod 600 "$ENV_FILE"

# Pick init system
if command -v systemctl >/dev/null 2>&1; then
    cat > /etc/systemd/system/fleet-agent.service <<'UNIT'
[Unit]
Description=Fleet agent — homelab-stats push collector
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/fleet-agent.env
ExecStart=/usr/local/bin/fleet-agent
Restart=always
RestartSec=10
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=read-only
PrivateTmp=true

[Install]
WantedBy=multi-user.target
UNIT
    systemctl daemon-reload
    systemctl enable --now fleet-agent.service
    sleep 2
    systemctl status fleet-agent.service --no-pager | head -20 || true
elif command -v rc-service >/dev/null 2>&1; then
    cat > /etc/init.d/fleet-agent <<'INITD'
#!/sbin/openrc-run

name="fleet-agent"
description="Fleet agent — homelab-stats push collector"
command="/usr/local/bin/fleet-agent"
command_background="yes"
pidfile="/run/fleet-agent.pid"
output_log="/var/log/fleet-agent.log"
error_log="/var/log/fleet-agent.log"

depend() {
    need net
    after firewall
}

start_pre() {
    [ -r /etc/fleet-agent.env ] || { eerror "Missing /etc/fleet-agent.env"; return 1; }
    set -a
    . /etc/fleet-agent.env
    set +a
    export FLEET_SERVER FLEET_SERVER_FALLBACK FLEET_TAGS FLEET_INTERVAL FLEET_TAILSCALE_IP FLEET_DISPLAY_NAME FLEET_BOOTSTRAP_TOKEN
}
INITD
    chmod +x /etc/init.d/fleet-agent
    rc-update add fleet-agent default
    rc-service fleet-agent restart
    sleep 2
    rc-service fleet-agent status || true
else
    echo "no supported init found (need systemd or openrc)" >&2
    exit 1
fi

# Clear bootstrap from env file after first run (state.json holds api_key now).
# We do this after one successful sample to avoid leaving the token on disk
# longer than necessary. The agent will overwrite state.json with the api_key
# on first registration; we then strip the bootstrap line.
sleep 6
if [ -f /var/lib/fleet-agent/state.json ] && grep -q api_key /var/lib/fleet-agent/state.json; then
    sed -i '/^FLEET_BOOTSTRAP_TOKEN=/d' "$ENV_FILE"
    echo "bootstrap token stripped from $ENV_FILE (agent is registered)"
    if command -v systemctl >/dev/null 2>&1; then
        systemctl restart fleet-agent.service
    else
        rc-service fleet-agent restart
    fi
fi

echo
echo "✓ fleet-agent installed and started"
echo "  binary:  $DEST"
echo "  env:     $ENV_FILE"
echo "  state:   /var/lib/fleet-agent/state.json"
