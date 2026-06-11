//go:build linux

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	prevCPUMu sync.Mutex
	prevCPU   []cpuTimes
)

type cpuTimes struct {
	user, nice, system, idle, iowait, irq, softirq, steal uint64
}

func (t cpuTimes) total() uint64 {
	return t.user + t.nice + t.system + t.idle + t.iowait + t.irq + t.softirq + t.steal
}

func (t cpuTimes) idleTotal() uint64 {
	return t.idle + t.iowait
}

func collectPlatform(s *sample) {
	if d := readDistroName(); d != "" {
		s.Distro = d
	}
	if k := readKernelVersion(); k != "" {
		s.Kernel = k
	}
	s.Uptime = readUptime()
	s.CPU = readCPU()
	s.Mem = readMem()
	s.Disks = readDisks()
	s.Net = readNet()
	if g := readGPUs(); len(g) > 0 {
		s.GPU = g
	}
	if b := readBattery(); b != nil {
		s.Battery = b
	}
	if svc := readServices(); len(svc) > 0 {
		s.Services = svc
	}
	if c := readContainers(); len(c) > 0 {
		s.Containers = c
	}
	if z := readZFS(); len(z) > 0 {
		s.ZFSPools = z
	}
	if sm := readSMART(); len(sm) > 0 {
		s.SMART = sm
	}
	if l := readRecentLogs(); len(l) > 0 {
		s.Logs = l
	}
	if k := readK3sStat(); k != nil {
		s.K3s = k
	}
}

func readDistroName() string {
	data, err := os.ReadFile("/etc/os-release")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "PRETTY_NAME=") {
			v := strings.TrimPrefix(line, "PRETTY_NAME=")
			return strings.Trim(v, "\"")
		}
	}
	return ""
}

func readKernelVersion() string {
	data, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func readUptime() int64 {
	data, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0
	}
	parts := strings.Fields(string(data))
	if len(parts) < 1 {
		return 0
	}
	f, _ := strconv.ParseFloat(parts[0], 64)
	return int64(f)
}

func parseCPUStat() ([]cpuTimes, error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil, err
	}
	var out []cpuTimes
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "cpu") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		nums := make([]uint64, 0, 8)
		for _, f := range fields[1:9] {
			n, _ := strconv.ParseUint(f, 10, 64)
			nums = append(nums, n)
		}
		for len(nums) < 8 {
			nums = append(nums, 0)
		}
		out = append(out, cpuTimes{
			user: nums[0], nice: nums[1], system: nums[2], idle: nums[3],
			iowait: nums[4], irq: nums[5], softirq: nums[6], steal: nums[7],
		})
	}
	return out, nil
}

func readCPU() cpuStat {
	cur, err := parseCPUStat()
	if err != nil || len(cur) == 0 {
		return cpuStat{Cores: runtimeNumCPU()}
	}

	prevCPUMu.Lock()
	prev := prevCPU
	prevCPU = cur
	prevCPUMu.Unlock()

	stat := cpuStat{Cores: len(cur) - 1}
	if stat.Cores < 1 {
		stat.Cores = runtimeNumCPU()
	}

	if len(prev) == len(cur) {
		dTotal := cur[0].total() - prev[0].total()
		dIdle := cur[0].idleTotal() - prev[0].idleTotal()
		if dTotal > 0 {
			stat.Pct = float64(dTotal-dIdle) / float64(dTotal) * 100
		}
		for i := 1; i < len(cur); i++ {
			dT := cur[i].total() - prev[i].total()
			dI := cur[i].idleTotal() - prev[i].idleTotal()
			pct := 0.0
			if dT > 0 {
				pct = float64(dT-dI) / float64(dT) * 100
			}
			stat.PerCore = append(stat.PerCore, roundN(pct, 1))
		}
	}
	stat.Pct = roundN(stat.Pct, 1)

	if l, err := os.ReadFile("/proc/loadavg"); err == nil {
		fields := strings.Fields(string(l))
		if len(fields) >= 3 {
			stat.Load1, _ = strconv.ParseFloat(fields[0], 64)
			stat.Load5, _ = strconv.ParseFloat(fields[1], 64)
			stat.Load15, _ = strconv.ParseFloat(fields[2], 64)
		}
	}

	stat.FreqMHz = readCPUFreqMHz()
	if temp := readCPUTemp(); temp != nil {
		stat.Temp = temp
	}
	return stat
}

func runtimeNumCPU() int {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return 0
	}
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "processor") {
			n++
		}
	}
	return n
}

func readCPUFreqMHz() float64 {
	entries, err := filepath.Glob("/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq")
	if err != nil || len(entries) == 0 {
		return 0
	}
	total := 0.0
	count := 0
	for _, p := range entries {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		khz, _ := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
		if khz > 0 {
			total += khz / 1000
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return roundN(total/float64(count), 0)
}

func readCPUTemp() *float64 {
	for _, hwmon := range globOr("/sys/class/hwmon/hwmon*") {
		nameBytes, _ := os.ReadFile(filepath.Join(hwmon, "name"))
		name := strings.TrimSpace(string(nameBytes))
		if name != "coretemp" && name != "k10temp" && name != "cpu_thermal" {
			continue
		}
		inputs, _ := filepath.Glob(filepath.Join(hwmon, "temp*_input"))
		var max float64
		for _, p := range inputs {
			b, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			v, _ := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
			if v/1000 > max {
				max = v / 1000
			}
		}
		if max > 0 {
			r := roundN(max, 1)
			return &r
		}
	}
	for _, p := range globOr("/sys/class/thermal/thermal_zone*/temp") {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		v, _ := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
		if v > 0 {
			r := roundN(v/1000, 1)
			return &r
		}
	}
	return nil
}

func globOr(pattern string) []string {
	matches, _ := filepath.Glob(pattern)
	return matches
}

func readMem() memStat {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return memStat{}
	}
	values := map[string]uint64{}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		fields := strings.Fields(parts[1])
		if len(fields) == 0 {
			continue
		}
		v, _ := strconv.ParseUint(fields[0], 10, 64)
		values[k] = v * 1024
	}
	total := values["MemTotal"]
	free := values["MemFree"]
	avail := values["MemAvailable"]
	used := total - avail
	if avail == 0 {
		buffers := values["Buffers"]
		cached := values["Cached"]
		used = total - free - buffers - cached
	}
	pct := 0.0
	if total > 0 {
		pct = float64(used) / float64(total) * 100
	}
	return memStat{
		Used: used, Total: total, Available: avail, Pct: roundN(pct, 1),
		SwapUsed:  values["SwapTotal"] - values["SwapFree"],
		SwapTotal: values["SwapTotal"],
	}
}

var keepFS = map[string]bool{
	"ext2": true, "ext3": true, "ext4": true, "xfs": true, "btrfs": true,
	"zfs": true, "f2fs": true, "vfat": true, "exfat": true, "ntfs": true,
	"nfs": true, "nfs4": true, "cifs": true, "fuse.sshfs": true,
}

func readDisks() []diskStat {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []diskStat
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		device, mount, fs := fields[0], fields[1], fields[2]
		if !keepFS[fs] {
			continue
		}
		if seen[mount] {
			continue
		}
		seen[mount] = true
		var fsStat syscall.Statfs_t
		if err := syscall.Statfs(mount, &fsStat); err != nil {
			continue
		}
		bs := uint64(fsStat.Bsize)
		total := fsStat.Blocks * bs
		used := total - fsStat.Bfree*bs
		pct := 0.0
		if total > 0 {
			pct = float64(used) / float64(total) * 100
		}
		out = append(out, diskStat{
			Mount: mount, Device: device, FS: fs,
			Used: used, Total: total, Pct: roundN(pct, 1),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Mount < out[j].Mount })
	return out
}

func skipIface(name string) bool {
	if name == "lo" {
		return true
	}
	for _, p := range []string{"veth", "br-", "cali", "flannel.", "cni", "kube-ipvs", "docker"} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

func readNet() []netIface {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return nil
	}
	now := time.Now()
	var out []netIface
	prevNetMu.Lock()
	defer prevNetMu.Unlock()
	for _, raw := range strings.Split(string(data), "\n") {
		if !strings.Contains(raw, ":") {
			continue
		}
		parts := strings.SplitN(raw, ":", 2)
		name := strings.TrimSpace(parts[0])
		if skipIface(name) {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 16 {
			continue
		}
		rx, _ := strconv.ParseUint(fields[0], 10, 64)
		tx, _ := strconv.ParseUint(fields[8], 10, 64)
		entry := netIface{Iface: name, RxTotal: rx, TxTotal: tx, Up: ifaceUp(name)}
		entry.SpeedMbs = readNetSpeed(name)
		prev, ok := prevNet[name]
		if ok {
			dt := now.Sub(prev.ts).Seconds()
			if dt > 0 {
				entry.RxBps = roundN(float64(rx-prev.rx)/dt, 0)
				entry.TxBps = roundN(float64(tx-prev.tx)/dt, 0)
			}
		}
		prevNet[name] = netStateEntry{rx: rx, tx: tx, ts: now}
		out = append(out, entry)
	}
	return out
}

func ifaceUp(name string) bool {
	b, err := os.ReadFile("/sys/class/net/" + name + "/operstate")
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(b)) == "up"
}

func readNetSpeed(name string) int {
	b, err := os.ReadFile("/sys/class/net/" + name + "/speed")
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	if n < 0 {
		return 0
	}
	return n
}

func readGPUs() []gpuStat {
	var out []gpuStat
	if has("nvidia-smi") {
		out = append(out, readNvidiaGPUs()...)
	}
	return out
}

func readNvidiaGPUs() []gpuStat {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,power.limit,fan.speed,clocks.current.graphics,clocks.current.memory",
		"--format=csv,noheader,nounits")
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	var gpus []gpuStat
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := splitCSV(line)
		if len(fields) < 6 {
			continue
		}
		atoi := func(s string) float64 {
			s = strings.TrimSpace(s)
			f, _ := strconv.ParseFloat(s, 64)
			return f
		}
		g := gpuStat{
			Vendor:   "nvidia",
			Name:     strings.TrimSpace(fields[0]),
			UtilPct:  atoi(fields[1]),
			MemUsed:  uint64(atoi(fields[2])) * 1024 * 1024,
			MemTotal: uint64(atoi(fields[3])) * 1024 * 1024,
			Temp:     atoi(fields[4]),
			PowerW:   atoi(fields[5]),
		}
		if len(fields) > 6 {
			g.PowerLimitW = atoi(fields[6])
		}
		if len(fields) > 7 {
			g.FanPct = atoi(fields[7])
		}
		if len(fields) > 8 {
			g.ClockMHz = atoi(fields[8])
		}
		if len(fields) > 9 {
			g.MemClockMHz = atoi(fields[9])
		}
		gpus = append(gpus, g)
	}
	return gpus
}

func splitCSV(line string) []string {
	r := csv.NewReader(strings.NewReader(line))
	r.LazyQuotes = true
	fields, err := r.Read()
	if err != nil {
		return nil
	}
	return fields
}

func readBattery() *batteryStat {
	dirs, _ := filepath.Glob("/sys/class/power_supply/*")
	if len(dirs) == 0 {
		return nil
	}
	b := &batteryStat{}
	have := false
	for _, d := range dirs {
		typeBytes, _ := os.ReadFile(filepath.Join(d, "type"))
		t := strings.TrimSpace(string(typeBytes))
		switch t {
		case "Battery":
			capBytes, err := os.ReadFile(filepath.Join(d, "capacity"))
			if err == nil {
				pct, _ := strconv.ParseFloat(strings.TrimSpace(string(capBytes)), 64)
				b.Pct = pct
				b.Present = true
				have = true
			}
			if pn, err := os.ReadFile(filepath.Join(d, "power_now")); err == nil {
				w, _ := strconv.ParseFloat(strings.TrimSpace(string(pn)), 64)
				b.Wattage = roundN(w/1e6, 1)
			}
			if eb, err := os.ReadFile(filepath.Join(d, "energy_now")); err == nil {
				if pn, err := os.ReadFile(filepath.Join(d, "power_now")); err == nil {
					energy, _ := strconv.ParseFloat(strings.TrimSpace(string(eb)), 64)
					power, _ := strconv.ParseFloat(strings.TrimSpace(string(pn)), 64)
					if power > 0 {
						b.TimeRemainingS = int(energy / power * 3600)
					}
				}
			}
		case "Mains", "USB":
			online, err := os.ReadFile(filepath.Join(d, "online"))
			if err == nil && strings.TrimSpace(string(online)) == "1" {
				b.ACOnline = true
				have = true
			}
		}
	}
	if !have {
		return nil
	}
	return b
}

func readServices() []serviceStat {
	if has("systemctl") {
		return readSystemdServices()
	}
	if has("rc-status") {
		return readOpenRCServices()
	}
	return nil
}

func readSystemdServices() []serviceStat {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "systemctl", "list-units", "--type=service",
		"--state=running,failed,activating,deactivating", "--no-legend",
		"--plain", "--full", "--no-pager").Output()
	if err != nil {
		return nil
	}
	var svcs []serviceStat
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		svcs = append(svcs, serviceStat{
			Name:    strings.TrimSuffix(fields[0], ".service"),
			Status:  fields[2],
			Enabled: true,
		})
	}
	return svcs
}

func readOpenRCServices() []serviceStat {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "rc-status", "-a", "-q", "--no-color").Output()
	if err != nil {
		return nil
	}
	var svcs []serviceStat
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Runlevel") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 1 {
			continue
		}
		name := fields[0]
		status := "unknown"
		if len(fields) >= 3 {
			status = strings.Trim(fields[2], "[]")
		}
		mapped := "active"
		switch strings.ToLower(status) {
		case "started":
			mapped = "active"
		case "stopped":
			mapped = "stopped"
		case "crashed":
			mapped = "failed"
		}
		svcs = append(svcs, serviceStat{Name: name, Status: mapped, Enabled: true})
	}
	return svcs
}

func readContainers() []containerStat {
	for _, bin := range []string{"docker", "podman"} {
		if !has(bin) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, bin, "ps", "--format",
			"{{.Names}}\t{{.Image}}\t{{.Status}}").Output()
		if err != nil {
			continue
		}
		var cs []containerStat
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" {
				continue
			}
			fields := strings.Split(line, "\t")
			if len(fields) < 3 {
				continue
			}
			cs = append(cs, containerStat{Name: fields[0], Image: fields[1], Status: fields[2]})
		}
		return cs
	}
	return nil
}

func readZFS() []zfsPoolStat {
	if !has("zpool") {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "zpool", "list", "-H", "-o",
		"name,size,alloc,free,frag,cap,health").Output()
	if err != nil {
		return nil
	}
	var pools []zfsPoolStat
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Split(line, "\t")
		if len(fields) < 7 {
			continue
		}
		fragPct, _ := strconv.Atoi(strings.TrimSuffix(fields[4], "%"))
		capPct, _ := strconv.Atoi(strings.TrimSuffix(fields[5], "%"))
		pools = append(pools, zfsPoolStat{
			Name:  fields[0], Size: fields[1], Alloc: fields[2], Free: fields[3],
			Frag: fragPct, Cap: capPct, State: fields[6],
		})
	}
	statusCtx, cancelS := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelS()
	st, err := exec.CommandContext(statusCtx, "zpool", "status", "-x").Output()
	if err == nil && len(pools) > 0 && !strings.Contains(string(st), "all pools are healthy") {
		for i := range pools {
			pools[i].Errors = countZFSErrors(string(st), pools[i].Name)
		}
	}
	return pools
}

func countZFSErrors(status, pool string) int {
	inPool := false
	count := 0
	for _, line := range strings.Split(status, "\n") {
		l := strings.TrimSpace(line)
		if strings.HasPrefix(l, "pool:") {
			inPool = strings.Contains(l, pool)
			continue
		}
		if !inPool {
			continue
		}
		fields := strings.Fields(l)
		if len(fields) >= 5 {
			for _, f := range fields[2:5] {
				if n, err := strconv.Atoi(f); err == nil && n > 0 {
					count += n
				}
			}
		}
	}
	return count
}

func readSMART() []smartStat {
	if !has("smartctl") {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "smartctl", "--scan", "-j").Output()
	if err != nil && len(out) == 0 {
		out, err = exec.CommandContext(ctx, "smartctl", "--scan").Output()
		if err != nil {
			return nil
		}
		return scanSMARTText(out)
	}
	type dev struct {
		Name string `json:"name"`
		Type string `json:"type,omitempty"`
	}
	var resp struct {
		Devices []dev `json:"devices"`
	}
	_ = json.Unmarshal(out, &resp)
	if len(resp.Devices) == 0 {
		return scanSMARTText(out)
	}
	var drives []smartStat
	for _, d := range resp.Devices {
		s := readSMARTOne(d.Name, d.Type)
		if s != nil {
			drives = append(drives, *s)
		}
	}
	return drives
}

func scanSMARTText(b []byte) []smartStat {
	var drives []smartStat
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		drive := readSMARTOne(fields[0], "")
		if drive != nil {
			drives = append(drives, *drive)
		}
	}
	return drives
}

func readSMARTOne(dev, typ string) *smartStat {
	args := []string{"-A", "-H", "-i", "-j"}
	if typ != "" {
		args = append(args, "-d", typ)
	}
	args = append(args, dev)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "smartctl", args...).Output()
	if err != nil && len(out) == 0 {
		return nil
	}
	var parsed struct {
		ModelName   string `json:"model_name"`
		ModelFamily string `json:"model_family"`
		Temperature struct {
			Current *float64 `json:"current"`
		} `json:"temperature"`
		SMARTStatus struct {
			Passed bool `json:"passed"`
		} `json:"smart_status"`
		ATAAttrs struct {
			Table []struct {
				Name string `json:"name"`
				Raw  struct {
					Value uint64 `json:"value"`
				} `json:"raw"`
			} `json:"table"`
		} `json:"ata_smart_attributes"`
	}
	if err := json.Unmarshal(out, &parsed); err != nil {
		return nil
	}
	s := &smartStat{Device: dev}
	if parsed.ModelName != "" {
		s.Model = parsed.ModelName
	} else {
		s.Model = parsed.ModelFamily
	}
	if parsed.Temperature.Current != nil {
		t := *parsed.Temperature.Current
		s.Temp = &t
	}
	if parsed.SMARTStatus.Passed {
		s.Health = "PASSED"
	} else {
		s.Health = "FAILED"
	}
	for _, attr := range parsed.ATAAttrs.Table {
		switch strings.ToLower(attr.Name) {
		case "power_on_hours":
			h := int(attr.Raw.Value)
			s.Hours = &h
		case "reallocated_sector_ct", "reallocated_event_count":
			r := int(attr.Raw.Value)
			s.Reallocated = &r
		}
	}
	return s
}

func readRecentLogs() []logLine {
	if has("journalctl") {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "journalctl", "-p", "err", "-n", "20",
			"--since", "1 hour ago", "--no-pager", "-o", "short-iso").Output()
		if err != nil {
			return nil
		}
		var logs []logLine
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			if line == "" || strings.HasPrefix(line, "--") {
				continue
			}
			parts := strings.SplitN(line, " ", 3)
			if len(parts) < 3 {
				continue
			}
			ts := parseJournalTime(parts[0])
			rest := parts[2]
			unit := ""
			if idx := strings.Index(rest, ":"); idx > 0 {
				unit = rest[:idx]
				rest = strings.TrimSpace(rest[idx+1:])
			}
			logs = append(logs, logLine{TS: ts, Level: "err", Unit: unit, Message: rest})
		}
		return logs
	}
	return readOpenRCRecentLogs()
}

func readOpenRCRecentLogs() []logLine {
	if !has("dmesg") {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "dmesg", "-l", "err,crit,alert,emerg", "-T").Output()
	if err != nil {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	var logs []logLine
	for _, line := range lines {
		if line == "" {
			continue
		}
		logs = append(logs, logLine{TS: time.Now().Unix(), Level: "err", Unit: "kernel", Message: line})
	}
	return logs
}

func parseJournalTime(s string) int64 {
	for _, layout := range []string{
		"2006-01-02T15:04:05-0700",
		"2006-01-02T15:04:05Z",
		time.RFC3339,
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Unix()
		}
	}
	return time.Now().Unix()
}

func readK3sStat() *k3sStat {
	if !has("k3s") && !has("kubectl") {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bin := "kubectl"
	args := []string{"get", "nodes", "-o", "wide", "--no-headers"}
	if has("k3s") && !has("kubectl") {
		bin = "k3s"
		args = append([]string{"kubectl"}, args...)
	}
	out, err := exec.CommandContext(ctx, bin, args...).Output()
	if err != nil {
		return nil
	}
	hostname, _ := os.Hostname()
	k := &k3sStat{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[0] == hostname {
			k.NodeName = fields[0]
			k.Ready = strings.Contains(fields[1], "Ready")
		}
	}
	if k.NodeName == "" {
		return nil
	}
	return k
}

func roundN(v float64, digits int) float64 {
	mult := 1.0
	for i := 0; i < digits; i++ {
		mult *= 10
	}
	if v >= 0 {
		return float64(int64(v*mult+0.5)) / mult
	}
	return float64(int64(v*mult-0.5)) / mult
}
