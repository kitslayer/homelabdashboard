package main

import (
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

// sample is the JSON payload the agent POSTs to /api/fleet/v1/ingest. The
// server stores the whole thing in a jsonb column and indexes specific fields
// for charts and alerts.
type sample struct {
	HostUUID     string         `json:"host_uuid"`
	TS           int64          `json:"ts"`
	Hostname     string         `json:"hostname"`
	OS           string         `json:"os"`
	Arch         string         `json:"arch"`
	Distro       string         `json:"distro,omitempty"`
	Kernel       string         `json:"kernel,omitempty"`
	AgentVersion string         `json:"agent_version"`
	Uptime       int64          `json:"uptime"`
	CPU          cpuStat        `json:"cpu"`
	Mem          memStat        `json:"mem"`
	Disks        []diskStat     `json:"disks"`
	Net          []netIface     `json:"net"`
	GPU          []gpuStat      `json:"gpu,omitempty"`
	Battery      *batteryStat   `json:"battery,omitempty"`
	Services     []serviceStat  `json:"services,omitempty"`
	Containers   []containerStat `json:"containers,omitempty"`
	ZFSPools     []zfsPoolStat  `json:"zfs_pools,omitempty"`
	SMART        []smartStat    `json:"smart,omitempty"`
	Logs         []logLine      `json:"logs,omitempty"`
	K3s          *k3sStat       `json:"k3s,omitempty"`
}

type cpuStat struct {
	Pct     float64   `json:"pct"`
	Cores   int       `json:"cores"`
	PerCore []float64 `json:"per_core,omitempty"`
	Load1   float64   `json:"load1,omitempty"`
	Load5   float64   `json:"load5,omitempty"`
	Load15  float64   `json:"load15,omitempty"`
	FreqMHz float64   `json:"freq_mhz,omitempty"`
	Temp    *float64  `json:"temp,omitempty"`
}

type memStat struct {
	Used      uint64  `json:"used"`
	Total     uint64  `json:"total"`
	Available uint64  `json:"available"`
	Pct       float64 `json:"pct"`
	SwapUsed  uint64  `json:"swap_used,omitempty"`
	SwapTotal uint64  `json:"swap_total,omitempty"`
}

type diskStat struct {
	Mount  string  `json:"mount"`
	Device string  `json:"device,omitempty"`
	FS     string  `json:"fs,omitempty"`
	Used   uint64  `json:"used"`
	Total  uint64  `json:"total"`
	Pct    float64 `json:"pct"`
}

type netIface struct {
	Iface    string  `json:"iface"`
	RxBps    float64 `json:"rx_bps"`
	TxBps    float64 `json:"tx_bps"`
	RxTotal  uint64  `json:"rx_total"`
	TxTotal  uint64  `json:"tx_total"`
	SpeedMbs int     `json:"speed_mbps,omitempty"`
	Up       bool    `json:"up"`
}

type gpuStat struct {
	Name        string  `json:"name"`
	Vendor      string  `json:"vendor"`
	UtilPct     float64 `json:"util_pct"`
	MemUsed     uint64  `json:"mem_used"`
	MemTotal    uint64  `json:"mem_total"`
	Temp        float64 `json:"temp"`
	PowerW      float64 `json:"power_w"`
	PowerLimitW float64 `json:"power_limit_w,omitempty"`
	FanPct      float64 `json:"fan_pct,omitempty"`
	ClockMHz    float64 `json:"clock_mhz,omitempty"`
	MemClockMHz float64 `json:"mem_clock_mhz,omitempty"`
}

type batteryStat struct {
	Present        bool    `json:"present"`
	Pct            float64 `json:"pct"`
	ACOnline       bool    `json:"ac_online"`
	TimeRemainingS int     `json:"time_remaining_s,omitempty"`
	Wattage        float64 `json:"wattage,omitempty"`
}

type serviceStat struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Enabled bool   `json:"enabled"`
}

type containerStat struct {
	Name    string  `json:"name"`
	Image   string  `json:"image,omitempty"`
	Status  string  `json:"status"`
	CPUPct  float64 `json:"cpu_pct,omitempty"`
	MemUsed uint64  `json:"mem_used,omitempty"`
}

type zfsPoolStat struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Size   string `json:"size,omitempty"`
	Alloc  string `json:"alloc,omitempty"`
	Free   string `json:"free,omitempty"`
	Frag   int    `json:"frag,omitempty"`
	Cap    int    `json:"cap,omitempty"`
	Errors int    `json:"errors,omitempty"`
}

type smartStat struct {
	Device     string  `json:"device"`
	Model      string  `json:"model,omitempty"`
	Temp       *float64 `json:"temp,omitempty"`
	Hours      *int    `json:"hours,omitempty"`
	Health     string  `json:"health,omitempty"`
	Reallocated *int   `json:"reallocated,omitempty"`
}

type logLine struct {
	TS      int64  `json:"ts"`
	Level   string `json:"level,omitempty"`
	Unit    string `json:"unit,omitempty"`
	Message string `json:"message"`
}

type k3sStat struct {
	NodeName string         `json:"node_name,omitempty"`
	Pods     []map[string]any `json:"pods,omitempty"`
	Ready    bool           `json:"ready"`
}

// netStateCache holds previous tx/rx so we can compute rates.
type netStateEntry struct {
	rx, tx uint64
	ts     time.Time
}

var (
	prevNetMu sync.Mutex
	prevNet   = map[string]netStateEntry{}
)

func collect(st state, cfg config) sample {
	host, _ := os.Hostname()
	now := time.Now().Unix()
	s := sample{
		HostUUID:     st.HostUUID,
		TS:           now,
		Hostname:     host,
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		AgentVersion: agentVersion,
	}
	s.Distro = readDistroName()
	s.Kernel = readKernelVersion()
	collectPlatform(&s)
	return s
}

// has reports whether a binary is on $PATH.
func has(bin string) bool {
	_, err := exec.LookPath(bin)
	return err == nil
}

func primaryIPv4() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip := addrToIP(addr)
			if ip == nil || ip.IsLoopback() || ip.To4() == nil {
				continue
			}
			if strings.HasPrefix(ip.String(), "169.254.") {
				continue
			}
			return ip.String()
		}
	}
	return ""
}

func addrToIP(a net.Addr) net.IP {
	switch v := a.(type) {
	case *net.IPNet:
		return v.IP
	case *net.IPAddr:
		return v.IP
	}
	return nil
}

func tailRune(b []byte) []byte {
	// strip trailing whitespace/newlines
	i := len(b)
	for i > 0 {
		c := b[i-1]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i--
			continue
		}
		break
	}
	return b[:i]
}
