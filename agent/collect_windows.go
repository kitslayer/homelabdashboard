//go:build windows

package main

import (
	"context"
	"encoding/csv"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	prevCPUMu sync.Mutex
)

func collectPlatform(s *sample) {
	s.Distro = "Windows"
	s.Kernel = readKernelVersion()
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
}

func readDistroName() string { return "Windows" }

func readKernelVersion() string {
	out, err := psOut(`(Get-CimInstance Win32_OperatingSystem).Version`)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func readUptime() int64 {
	out, err := psOut(`(Get-Date) - (Get-CimInstance Win32_OperatingSystem).LastBootUpTime | %{ [int]$_.TotalSeconds }`)
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	return n
}

func readCPU() cpuStat {
	out, err := psOut(`$c = Get-CimInstance Win32_Processor;
$pct = ($c | Measure-Object -Property LoadPercentage -Average).Average;
$cores = ($c | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum;
$mhz = ($c | Measure-Object -Property MaxClockSpeed -Average).Average;
"$pct,$cores,$mhz"`)
	stat := cpuStat{}
	if err == nil {
		fields := strings.Split(strings.TrimSpace(out), ",")
		if len(fields) >= 3 {
			pct, _ := strconv.ParseFloat(fields[0], 64)
			cores, _ := strconv.Atoi(fields[1])
			mhz, _ := strconv.ParseFloat(fields[2], 64)
			stat.Pct = roundN(pct, 1)
			stat.Cores = cores
			stat.FreqMHz = mhz
		}
	}
	if t := readCPUTemp(); t != nil {
		stat.Temp = t
	}
	return stat
}

func readCPUTemp() *float64 {
	out, err := psOut(`try { $t = (Get-CimInstance -Namespace root/wmi -ClassName MSAcpi_ThermalZoneTemperature -ErrorAction Stop).CurrentTemperature; if ($t) { ($t / 10 - 273.15) } } catch {}`)
	if err != nil || strings.TrimSpace(out) == "" {
		return nil
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(out), 64)
	if err != nil {
		return nil
	}
	r := roundN(v, 1)
	return &r
}

func readMem() memStat {
	out, err := psOut(`$os = Get-CimInstance Win32_OperatingSystem;
$total = [int64]$os.TotalVisibleMemorySize * 1024;
$free = [int64]$os.FreePhysicalMemory * 1024;
"$total,$free"`)
	if err != nil {
		return memStat{}
	}
	fields := strings.Split(strings.TrimSpace(out), ",")
	if len(fields) < 2 {
		return memStat{}
	}
	total, _ := strconv.ParseUint(fields[0], 10, 64)
	free, _ := strconv.ParseUint(fields[1], 10, 64)
	used := total - free
	pct := 0.0
	if total > 0 {
		pct = float64(used) / float64(total) * 100
	}
	return memStat{Used: used, Total: total, Available: free, Pct: roundN(pct, 1)}
}

func readDisks() []diskStat {
	out, err := psOut(`Get-PSDrive -PSProvider FileSystem | Where-Object { $_.Used -or $_.Free } | ForEach-Object { "$($_.Name),$($_.Used),$($_.Free)" }`)
	if err != nil {
		return nil
	}
	var disks []diskStat
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Split(strings.TrimSpace(line), ",")
		if len(fields) < 3 {
			continue
		}
		used, _ := strconv.ParseUint(fields[1], 10, 64)
		free, _ := strconv.ParseUint(fields[2], 10, 64)
		total := used + free
		pct := 0.0
		if total > 0 {
			pct = float64(used) / float64(total) * 100
		}
		disks = append(disks, diskStat{Mount: fields[0] + ":\\", Used: used, Total: total, Pct: roundN(pct, 1)})
	}
	return disks
}

func readNet() []netIface {
	out, err := psOut(`Get-NetAdapter | Where-Object { $_.Status -eq 'Up' } | ForEach-Object {
$s = Get-NetAdapterStatistics -Name $_.Name -ErrorAction SilentlyContinue
"$($_.Name)|$($_.LinkSpeed)|$($_.Status)|$($s.ReceivedBytes)|$($s.SentBytes)"
}`)
	if err != nil {
		return nil
	}
	now := time.Now()
	prevNetMu.Lock()
	defer prevNetMu.Unlock()
	var ifaces []netIface
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Split(strings.TrimSpace(line), "|")
		if len(fields) < 5 {
			continue
		}
		name := fields[0]
		rx, _ := strconv.ParseUint(fields[3], 10, 64)
		tx, _ := strconv.ParseUint(fields[4], 10, 64)
		entry := netIface{Iface: name, Up: fields[2] == "Up", RxTotal: rx, TxTotal: tx}
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

func readGPUs() []gpuStat {
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
		"--format=csv,noheader,nounits").Output()
	if err != nil {
		return nil
	}
	var gpus []gpuStat
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		r := csv.NewReader(strings.NewReader(line))
		fields, err := r.Read()
		if err != nil || len(fields) < 6 {
			continue
		}
		atof := func(s string) float64 { f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64); return f }
		gpus = append(gpus, gpuStat{
			Vendor:   "nvidia",
			Name:     strings.TrimSpace(fields[0]),
			UtilPct:  atof(fields[1]),
			MemUsed:  uint64(atof(fields[2])) * 1024 * 1024,
			MemTotal: uint64(atof(fields[3])) * 1024 * 1024,
			Temp:     atof(fields[4]),
			PowerW:   atof(fields[5]),
		})
	}
	return gpus
}

func readBattery() *batteryStat {
	out, err := psOut(`$b = Get-CimInstance Win32_Battery -ErrorAction SilentlyContinue;
if ($b) { "$($b.EstimatedChargeRemaining),$($b.BatteryStatus)" } else { "" }`)
	if err != nil || strings.TrimSpace(out) == "" {
		return nil
	}
	fields := strings.Split(strings.TrimSpace(out), ",")
	if len(fields) < 2 {
		return nil
	}
	pct, _ := strconv.ParseFloat(fields[0], 64)
	statusCode, _ := strconv.Atoi(fields[1])
	return &batteryStat{Present: true, Pct: pct, ACOnline: statusCode == 2}
}

func readServices() []serviceStat {
	out, err := psOut(`Get-Service | Where-Object { $_.Status -ne 'Stopped' } | ForEach-Object { "$($_.Name)|$($_.Status)|$($_.StartType)" }`)
	if err != nil {
		return nil
	}
	var svcs []serviceStat
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		fields := strings.Split(strings.TrimSpace(line), "|")
		if len(fields) < 2 {
			continue
		}
		st := "stopped"
		if strings.EqualFold(fields[1], "Running") {
			st = "active"
		}
		svcs = append(svcs, serviceStat{Name: fields[0], Status: st, Enabled: true})
	}
	return svcs
}

func psOut(script string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "powershell", "-NoProfile", "-NonInteractive", "-Command", script)
	out, err := cmd.Output()
	return string(out), err
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
