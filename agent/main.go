// fleet-agent — push host metrics to homelab-stats fleet API every N seconds.
//
// Cross-platform: stdlib only. On Linux it parses /proc and /sys plus optional
// helper binaries (nvidia-smi, zpool, smartctl, systemctl). On Windows it shells
// out to Get-Counter / WMI. macOS is best-effort using sysctl + pmset.
//
// Configuration (in priority order: flag > env > built-in default):
//   --server=URL        / FLEET_SERVER         e.g. http://<SERVER>/
//   --fallback=URL      / FLEET_SERVER_FALLBACK e.g. https://milescoviello.com
//   --bootstrap=TOKEN   / FLEET_BOOTSTRAP_TOKEN one-shot, dropped after register
//   --state=PATH        / FLEET_STATE          default ~/.fleet-agent.json
//   --tags=foo,bar      / FLEET_TAGS
//   --interval=30       / FLEET_INTERVAL
//   --tailscale-ip=X    / FLEET_TAILSCALE_IP   optional explicit override
//   --version           print build info
//
// Once registered, the agent persists host_uuid + api_key to the state file and
// no longer needs the bootstrap token. Re-registering rotates the api_key.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

const agentVersion = "0.1.0"

type state struct {
	HostUUID string `json:"host_uuid"`
	APIKey   string `json:"api_key"`
	Server   string `json:"server"`
}

type config struct {
	servers      []string
	bootstrap    string
	statePath    string
	tags         string
	interval     time.Duration
	tailscaleIP  string
	displayName  string
	skipInsecure bool
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func main() {
	var (
		server      = flag.String("server", envOr("FLEET_SERVER", ""), "primary server URL")
		fallback    = flag.String("fallback", envOr("FLEET_SERVER_FALLBACK", ""), "fallback server URL")
		bootstrap   = flag.String("bootstrap", envOr("FLEET_BOOTSTRAP_TOKEN", ""), "bootstrap token (registration only)")
		statePath   = flag.String("state", envOr("FLEET_STATE", defaultStatePath()), "state file path")
		tags        = flag.String("tags", envOr("FLEET_TAGS", ""), "comma-separated tags")
		intervalSec = flag.Int("interval", atoiOr(os.Getenv("FLEET_INTERVAL"), 30), "push interval seconds")
		tailscaleIP = flag.String("tailscale-ip", envOr("FLEET_TAILSCALE_IP", ""), "advertised tailscale ip")
		displayName = flag.String("display-name", envOr("FLEET_DISPLAY_NAME", ""), "optional display name")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("fleet-agent %s %s/%s\n", agentVersion, runtime.GOOS, runtime.GOARCH)
		return
	}

	if *server == "" {
		fmt.Fprintln(os.Stderr, "fleet-agent: --server (or FLEET_SERVER) is required")
		os.Exit(2)
	}

	cfg := config{
		servers:     filterEmpty([]string{*server, *fallback}),
		bootstrap:   *bootstrap,
		statePath:   *statePath,
		tags:        *tags,
		interval:    time.Duration(*intervalSec) * time.Second,
		tailscaleIP: *tailscaleIP,
		displayName: *displayName,
	}
	if cfg.interval < 5*time.Second {
		cfg.interval = 5 * time.Second
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "fleet-agent:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	st, err := loadOrInitState(cfg.statePath)
	if err != nil {
		return fmt.Errorf("state: %w", err)
	}

	client := &http.Client{Timeout: 15 * time.Second}

	if st.APIKey == "" {
		if cfg.bootstrap == "" {
			return errors.New("no api_key in state and no --bootstrap provided; can't register")
		}
		if err := register(ctx, client, &st, cfg); err != nil {
			return fmt.Errorf("register: %w", err)
		}
		if err := saveState(cfg.statePath, st); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		fmt.Printf("fleet-agent: registered host_uuid=%s server=%s\n", st.HostUUID, st.Server)
	}

	tick := time.NewTicker(cfg.interval)
	defer tick.Stop()

	// First sample immediately for fast feedback.
	pushOnce(ctx, client, &st, cfg)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			pushOnce(ctx, client, &st, cfg)
		}
	}
}

func pushOnce(ctx context.Context, client *http.Client, st *state, cfg config) {
	sample := collect(*st, cfg)
	body, err := json.Marshal(sample)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fleet-agent: marshal:", err)
		return
	}
	// Try servers in order. On success, remember last working server.
	servers := orderedServers(cfg.servers, st.Server)
	var lastErr error
	for _, s := range servers {
		err := postIngest(ctx, client, s, st.APIKey, body)
		if err == nil {
			if st.Server != s {
				st.Server = s
				_ = saveState(cfg.statePath, *st)
			}
			return
		}
		lastErr = err
	}
	fmt.Fprintln(os.Stderr, "fleet-agent: push failed:", lastErr)
}

func orderedServers(all []string, preferred string) []string {
	if preferred == "" {
		return all
	}
	out := []string{preferred}
	for _, s := range all {
		if s != preferred {
			out = append(out, s)
		}
	}
	return out
}

func postIngest(ctx context.Context, client *http.Client, server, apiKey string, body []byte) error {
	url := strings.TrimRight(server, "/") + "/api/fleet/v1/ingest"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("User-Agent", "fleet-agent/"+agentVersion)
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fmt.Errorf("%s: %s", res.Status, strings.TrimSpace(string(b)))
	}
	io.Copy(io.Discard, res.Body)
	return nil
}

type registerResp struct {
	HostID   int64  `json:"host_id"`
	HostUUID string `json:"host_uuid"`
	APIKey   string `json:"api_key"`
}

func register(ctx context.Context, client *http.Client, st *state, cfg config) error {
	if st.HostUUID == "" {
		st.HostUUID = newUUID()
	}
	host, _ := os.Hostname()
	payload := map[string]any{
		"host_uuid":     st.HostUUID,
		"hostname":      host,
		"display_name":  cfg.displayName,
		"os":            runtime.GOOS,
		"arch":          runtime.GOARCH,
		"agent_version": agentVersion,
		"tags":          cfg.tags,
		"tailscale_ip":  cfg.tailscaleIP,
		"ip":            primaryIPv4(),
		"distro":        readDistroName(),
		"kernel":        readKernelVersion(),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	servers := cfg.servers
	if st.Server != "" {
		servers = orderedServers(cfg.servers, st.Server)
	}
	var lastErr error
	for _, s := range servers {
		req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(s, "/")+"/api/fleet/v1/register", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+cfg.bootstrap)
		req.Header.Set("User-Agent", "fleet-agent/"+agentVersion)
		res, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		res.Body.Close()
		if res.StatusCode >= 300 {
			lastErr = fmt.Errorf("%s: %s", s, strings.TrimSpace(string(raw)))
			continue
		}
		var rr registerResp
		if err := json.Unmarshal(raw, &rr); err != nil {
			lastErr = fmt.Errorf("decode: %w", err)
			continue
		}
		if rr.APIKey == "" {
			lastErr = fmt.Errorf("server returned empty api_key from %s", s)
			continue
		}
		st.HostUUID = rr.HostUUID
		st.APIKey = rr.APIKey
		st.Server = s
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("no servers configured")
	}
	return lastErr
}

func newUUID() string {
	var b [16]byte
	_, err := rand.Read(b[:])
	if err != nil {
		// Fallback: time-based. Crypto/rand failing is essentially impossible.
		now := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(now >> (i * 4))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]))
}

func loadOrInitState(path string) (state, error) {
	st := state{}
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, &st)
		if err != nil {
			return st, fmt.Errorf("bad state file: %w", err)
		}
		return st, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return st, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return st, err
	}
	return st, nil
}

func saveState(path string, st state) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func defaultStatePath() string {
	if v := os.Getenv("FLEET_STATE"); v != "" {
		return v
	}
	if runtime.GOOS == "windows" {
		base := os.Getenv("ProgramData")
		if base == "" {
			base = "C:\\ProgramData"
		}
		return filepath.Join(base, "fleet-agent", "state.json")
	}
	// Prefer /var/lib if running as root, else home.
	if os.Geteuid() == 0 {
		return "/var/lib/fleet-agent/state.json"
	}
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "/tmp"
	}
	return filepath.Join(home, ".fleet-agent.json")
}

func filterEmpty(in []string) []string {
	out := in[:0]
	for _, s := range in {
		if strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func atoiOr(s string, def int) int {
	if s == "" {
		return def
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	if n == 0 {
		return def
	}
	return n
}
