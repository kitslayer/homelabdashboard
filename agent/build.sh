#!/usr/bin/env bash
# Build fleet-agent for every supported target into ./dist/.
set -euo pipefail

cd "$(dirname "$0")"
rm -rf dist
mkdir -p dist

build() {
    local goos="$1" goarch="$2" suffix="${3:-}"
    local name="fleet-agent-${goos}-${goarch}${suffix}"
    local out="dist/${name}"
    echo "→ ${name}"
    GOOS="$goos" GOARCH="$goarch" CGO_ENABLED=0 \
        go build -trimpath -ldflags='-s -w' -o "$out" .
}

build linux amd64
build linux arm64
build linux 386
build linux arm
build windows amd64 .exe
build darwin amd64
build darwin arm64

# Strip on Linux if available (already done via -s -w but keep symmetry).
echo "→ build complete"
ls -lh dist
