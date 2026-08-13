import SwiftUI

struct HostRef: Identifiable { var id: String }   // for .sheet(item:)

struct HostDetailView: View {
    @EnvironmentObject var store: ArgusStore
    var name: String
    @Environment(\.dismiss) private var dismiss

    @State private var detail: HostDetailResponse?
    @State private var err: String?
    @State private var msg: String?
    @State private var msgErr = false
    @State private var histMetric = "cpu"
    @State private var histWindow = "hour"
    @State private var hist: HistoryResponse?

    private let kvCols = [GridItem(.adaptive(minimum: 108), spacing: 8)]

    var body: some View {
        NavigationStack {
            ZStack {
                AtmosphereBackground()
                ScrollView {
                    VStack(spacing: 12) {
                        if let d = detail { content(d) }
                        else if let e = err { ErrPanel(message: e) }
                        else { ProgressView().tint(Theme.amber).frame(maxWidth: .infinity).padding(.vertical, 50) }
                    }
                    .padding(16)
                }
            }
            .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Close") { dismiss() } } }
        }
        .preferredColorScheme(.dark)
        .task {
            while !Task.isCancelled { await load(); try? await Task.sleep(nanoseconds: 8_000_000_000) }
        }
    }

    private func load() async {
        do { detail = try await store.hostDetail(name); err = nil } catch { err = error.localizedDescription }
    }

    @ViewBuilder private func content(_ d: HostDetailResponse) -> some View {
        let h = d.host
        // header + actions
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(h.label).font(Theme.display(20)).tracking(1).foregroundStyle(Theme.ink).lineLimit(2)
                Spacer()
                StatusChip(h.up ? "UP" : "DOWN", color: h.up ? Theme.green : Theme.red)
            }
            Text([h.ip, h.note].compactMap { ($0?.isEmpty == false) ? $0 : nil }.joined(separator: " · "))
                .font(Theme.mono(11)).foregroundStyle(Theme.inkFaint)
            powerButtons(h)
            if h.caps?.contains("binhost") == true {
                HoldButton(label: "BINPKG BUILD", hint: "HOLD", ms: 900, role: .normal) { await runBinpkg() }
            }
            if let msg {
                Text(msg).font(Theme.mono(11)).foregroundStyle(msgErr ? Theme.red : Theme.green)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .argusPanel()

        if let f = h.fleet {
            PanelSection(title: "TELEMETRY", systemImage: "gauge.with.dots.needle.bottom.50percent") {
                LazyVGrid(columns: kvCols, spacing: 8) {
                    ForEach(kv(h, f), id: \.0) { k, v in
                        VStack(alignment: .leading, spacing: 3) {
                            Text(k).font(Theme.display(9)).tracking(1.4).foregroundStyle(Theme.inkFaint)
                            Text(v).font(Theme.mono(13)).foregroundStyle(Theme.ink).lineLimit(1).minimumScaleFactor(0.6)
                        }
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(10).background(Theme.bgRaise)
                    }
                }
                VStack(spacing: 8) {
                    if let c = f.cpu_pct { Gauge(label: "CPU", pct: c) }
                    if let m = f.mem_pct { Gauge(label: "MEM", pct: m) }
                    if let dk = f.disk_root_pct { Gauge(label: "DISK", pct: dk, warn: 80, crit: 92) }
                    ForEach(Array((f.gpu ?? []).enumerated()), id: \.offset) { _, g in
                        Gauge(label: "GPU", pct: g.util_pct)
                        if let mt = g.mem_total, mt > 0, let mu = g.mem_used { Gauge(label: "VRAM", pct: mu / mt * 100) }
                    }
                }
                .padding(.top, 8)
            }
        }

        if h.fleet_id != nil {
            PanelSection(title: "HISTORY", systemImage: "chart.xyaxis.line") {
                VStack(spacing: 8) {
                    Segmented(options: HistoryMetric.order, selection: $histMetric)
                    Segmented(options: ["hour", "day", "week"], selection: $histWindow)
                    HistoryChart(response: hist, metric: histMetric)
                }
            }
            .task(id: "\(name)|\(histMetric)|\(histWindow)") {
                hist = try? await store.history(name, window: histWindow, metric: histMetric)
            }
        }

        if let disks = d.latest?.disks, !disks.isEmpty { disksPanel(disks) }
        if let smart = d.latest?.smart, !smart.isEmpty { smartPanel(smart) }
    }

    private func kv(_ h: HostView, _ f: FleetMetrics) -> [(String, String)] {
        var out: [(String, String)] = []
        out.append(("STATE", (h.up ? "UP" : "DOWN") + (h.source.map { " · \($0)" } ?? "")))
        if let p = h.ping_ms { out.append(("PING", String(format: "%.1f ms", p))) }
        if let u = f.uptime { out.append(("UPTIME", Fmt.uptime(u))) }
        if let l = f.load1 { out.append(("LOAD", String(format: "%.2f", l) + (f.cores.map { " / \($0)c" } ?? ""))) }
        if let t = f.cpu_temp { out.append(("CPU TEMP", Fmt.temp(t))) }
        if let mt = f.mem_total { out.append(("MEMORY", "\(Fmt.bytes(f.mem_used)) / \(Fmt.bytes(mt))")) }
        if let rx = f.net_rx_bps { out.append(("NET", "↓\(Fmt.bps(rx)) ↑\(Fmt.bps(f.net_tx_bps))")) }
        if let k = f.kernel { out.append(("KERNEL", k)) }
        if let ds = f.distro { out.append(("DISTRO", ds.replacingOccurrences(of: "'", with: ""))) }
        if let pve = h.pve { out.append(("PVE", "\(pve.status ?? "") · \(pve.node ?? "")")) }
        if let b = f.battery, let pct = b.pct { out.append(("BATTERY", "\(Int(pct))% " + ((b.ac_online ?? false) ? "AC" : "BATT"))) }
        for g in f.gpu ?? [] { out.append(("GPU", "\(Fmt.pct(g.util_pct)) · \(Fmt.temp(g.temp)) · \(g.power_w.map { String(format: "%.0fW", $0) } ?? "—")")) }
        return out
    }

    @ViewBuilder private func powerButtons(_ h: HostView) -> some View {
        HStack(spacing: 10) {
            if !h.up, h.can_wake == true {
                Button { Task { await run("wake") } } label: {
                    Label("WAKE", systemImage: "power").font(Theme.display(12)).tracking(1.2).foregroundStyle(Theme.amber)
                        .frame(maxWidth: .infinity, minHeight: 44)
                        .overlay(Chamfer(cut: 8).stroke(Theme.edgeHard, lineWidth: 1))
                }.buttonStyle(.plain)
            }
            if h.up, h.can_power == true {
                HoldButton(label: "REBOOT", ms: 900) { await run("reboot") }
                HoldButton(label: "SHUTDOWN", ms: 1300) { await run("shutdown") }
            }
        }
        .padding(.top, 4)
    }

    private func disksPanel(_ disks: [DiskInfo]) -> some View {
        // dedupe by device, prefer shortest mount
        var byDev: [String: DiskInfo] = [:]
        for d in disks {
            let key = d.device ?? d.mount
            if let prev = byDev[key], prev.mount.count <= d.mount.count { continue }
            byDev[key] = d
        }
        let rows = byDev.values.sorted { ($0.pct ?? 0) > ($1.pct ?? 0) }
        return PanelSection(title: "FILESYSTEMS", systemImage: "internaldrive", trailing: "\(rows.count)") {
            ForEach(rows) { d in
                VStack(alignment: .leading, spacing: 4) {
                    HStack {
                        Text(d.mount).font(Theme.mono(12)).foregroundStyle(Theme.ink).lineLimit(1)
                        Spacer()
                        Text("\(Fmt.bytes(d.used)) / \(Fmt.bytes(d.total))").font(Theme.mono(10)).foregroundStyle(Theme.inkFaint)
                    }
                    Gauge(label: "", pct: d.pct, warn: 78, crit: 90)
                }
                .padding(.vertical, 6)
            }
        }
    }

    private func smartPanel(_ smart: [SmartInfo]) -> some View {
        PanelSection(title: "DRIVE HEALTH (SMART)", systemImage: "stethoscope", trailing: "\(smart.count)") {
            ForEach(smart) { s in
                DotRow(up: (s.health ?? "").lowercased() == "passed",
                       name: s.device,
                       detail: s.model,
                       trailing: [s.temp.map { Fmt.temp($0) }, s.hours.map { "\($0)h" },
                                  (s.reallocated ?? 0) > 0 ? "RE:\(s.reallocated!)" : nil].compactMap { $0 }.joined(separator: " · "),
                       trailingColor: (s.reallocated ?? 0) > 0 ? Theme.red : Theme.inkDim)
            }
        }
    }

    private func run(_ op: String) async {
        do { try await store.hostPower(host: name, op: op); msg = "\(op) sent ✓"; msgErr = false }
        catch { msg = error.localizedDescription; msgErr = true }
        await load()
    }

    private func runBinpkg() async {
        do { try await store.binpkgBuild(); msg = "binpkg build started ✓"; msgErr = false }
        catch { msg = error.localizedDescription; msgErr = true }
    }
}
