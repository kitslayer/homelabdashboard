//go:build darwin

package main

import (
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	prevCPUMu sync.Mutex
)

func collectPlatform(s *sample) {
	s.Distro = readDistroName()
	s.Kernel = readKernelVersion()
	s.Uptime = readUptime()
	s.CPU = readCPU()
	s.Mem = readMem()
	s.Disks = readDisks()
	s.Net = readNet()
	if b := readBattery(); b != nil {
		s.Battery = b
	}
}

func readDistroName() string {
	out, err := exec.Command("sw_vers", "-productName").Output()
	if err != nil {
		return "macOS"
	}
	return strings.TrimSpace(string(out))
}

func readKernelVersion() string {
	out, err := exec.Command("uname", "-r").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func readUptime() int64 {
	out, err := exec.Command("sysctl", "-n", "kern.boottime").Output()
	if err != nil {
		return 0
	}
	s := string(out)
	idx := strings.Index(s, "sec = ")
	if idx < 0 {
		return 0
	}
	rest := s[idx+6:]
	end := strings.IndexAny(rest, ",")
	if end < 0 {
		return 0
	}
	boot, _ := strconv.ParseInt(strings.TrimSpace(rest[:end]), 10, 64)
	if boot == 0 {
		return 0
	}
	return time.Now().Unix() - boot
}

func readCPU() cpuStat {
	out, _ := exec.Command("sysctl", "-n", "hw.ncpu").Output()
	cores, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	pct := 0.0
	if data, err := exec.Command("top", "-l", "1", "-n", "0").Output(); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "CPU usage:") {
				parts := strings.Split(line, " ")
				for _, p := range parts {
					if strings.Contains(p, "%") {
						v, err := strconv.ParseFloat(strings.TrimSuffix(p, "%"), 64)
						if err == nil {
							pct = 100 - v // last percent is idle
							break
						}
					}
				}
			}
		}
	}
	return cpuStat{Pct: roundN(pct, 1), Cores: cores}
}

func readMem() memStat {
	out, _ := exec.Command("sysctl", "-n", "hw.memsize").Output()
	total, _ := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	used := uint64(0)
	if data, err := exec.Command("vm_stat").Output(); err == nil {
		var pageSize uint64 = 4096
		var freePages, activePages, wiredPages uint64
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.SplitN(line, ":", 2)
			if len(fields) != 2 {
				continue
			}
			v := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(fields[1]), "."))
			n, _ := strconv.ParseUint(v, 10, 64)
			switch strings.TrimSpace(fields[0]) {
			case "Pages free":
				freePages = n
			case "Pages active":
				activePages = n
			case "Pages wired down":
				wiredPages = n
			}
		}
		used = (activePages + wiredPages) * pageSize
		_ = freePages
	}
	pct := 0.0
	if total > 0 {
		pct = float64(used) / float64(total) * 100
	}
	return memStat{Used: used, Total: total, Pct: roundN(pct, 1)}
}

func readDisks() []diskStat {
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/", &stat); err != nil {
		return nil
	}
	bs := uint64(stat.Bsize)
	total := stat.Blocks * bs
	used := total - stat.Bfree*bs
	pct := float64(used) / float64(total) * 100
	return []diskStat{{Mount: "/", Total: total, Used: used, Pct: roundN(pct, 1)}}
}

func readNet() []netIface {
	out, err := exec.Command("netstat", "-ibn").Output()
	if err != nil {
		return nil
	}
	now := time.Now()
	prevNetMu.Lock()
	defer prevNetMu.Unlock()
	seen := map[string]bool{}
	var ifaces []netIface
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		name := fields[0]
		if name == "Name" || seen[name] {
			continue
		}
		seen[name] = true
		rx, _ := strconv.ParseUint(fields[6], 10, 64)
		tx, _ := strconv.ParseUint(fields[9], 10, 64)
		entry := netIface{Iface: name, RxTotal: rx, TxTotal: tx, Up: true}
		prev, ok := prevNet[name]
		if ok {
			dt := now.Sub(prev.ts).Seconds()
			if dt > 0 {
				entry.RxBps = roundN(float64(rx-prev.rx)/dt, 0)
				entry.TxBps = roundN(float64(tx-prev.tx)/dt, 0)
			}
		}
		prevNet[name] = netStateEntry{rx: rx, tx: tx, ts: now}
		ifaces = append(ifaces, entry)
	}
	return ifaces
}

func readBattery() *batteryStat {
	out, err := exec.Command("pmset", "-g", "batt").Output()
	if err != nil {
		return nil
	}
	s := string(out)
	if !strings.Contains(s, "InternalBattery") {
		return nil
	}
	b := &batteryStat{Present: true}
	if idx := strings.Index(s, "%"); idx > 0 {
		start := idx
		for start > 0 && s[start-1] >= '0' && s[start-1] <= '9' {
			start--
		}
		pct, _ := strconv.ParseFloat(s[start:idx], 64)
		b.Pct = pct
	}
	if strings.Contains(s, "AC Power") || strings.Contains(s, "charging") {
		b.ACOnline = true
	}
	return b
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

