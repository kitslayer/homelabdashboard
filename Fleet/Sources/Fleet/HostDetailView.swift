import SwiftUI
import Charts

struct HostDetailView: View {
    let hostID: Int
    @EnvironmentObject var fleet: FleetStore
    @State private var history: HistorySeries?
    @State private var window: String = "day"
    @State private var historyError: String?

    var host: Host? { fleet.host(id: hostID) }

    var body: some View {
        ZStack {
            Theme.bg.ignoresSafeArea()
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    if let host {
                        header(for: host)
                        summary(for: host)
                        if let latest = host.latest {
                            sections(for: host, latest: latest)
                        }
                        historySection(for: host)
                    } else {
                        Text("Host not found").foregroundStyle(.secondary)
                    }
                }
                .padding(.horizontal, 16)
                .padding(.bottom, 32)
            }
        }
        .navigationTitle(host?.displayName ?? "Host")
        .navigationBarTitleDisplayMode(.inline)
        .task {
            await loadHistory()
        }
        .onChange(of: window) { _, _ in
            Task { await loadHistory() }
        }
        .refreshable {
            await fleet.refresh()
            await loadHistory()
        }
    }

    private func header(for host: Host) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(host.displayName).font(.title3.bold())
            Text(host.hostname).font(.subheadline).foregroundStyle(.secondary)
            HStack(spacing: 6) {
                pill(host.up ? "UP" : "DOWN", color: host.up ? Theme.ok : Theme.crit)
                if let ip = host.ip { pill(ip, color: Theme.accent) }
                if let ts = host.tailscale_ip { pill("ts:\(ts)", color: Theme.info) }
                if let loc = host.location_tag { pill(loc, color: .purple) }
            }
            if let kernel = host.kernel {
                Text(kernel).font(.caption2).foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .fleetPanel()
    }

    private func pill(_ text: String, color: Color) -> some View {
        Text(text)
            .font(.caption2.weight(.semibold))
            .padding(.horizontal, 8).padding(.vertical, 3)
            .background(Capsule().fill(color.opacity(0.18)))
            .foregroundStyle(color)
    }

    private func summary(for host: Host) -> some View {
        let latest = host.latest
        let cpu = latest?.cpu
        let mem = latest?.mem
        let root = latest?.rootDisk
        return HStack(spacing: 10) {
            summaryTile("CPU", primary: cpu?.pct.map { "\(Int($0))%" } ?? "—",
                        secondary: cpu?.temp.map { "\(Int($0))°C" } ?? cpu?.cores.map { "\($0) cores" })
            summaryTile("Mem", primary: mem?.pct.map { "\(Int($0))%" } ?? "—",
                        secondary: mem?.total.map { "\(fmtBytes(mem?.used)) / \(fmtBytes($0))" })
            summaryTile("Disk", primary: root?.pct.map { "\(Int($0))%" } ?? "—",
                        secondary: root?.total.map { "\(fmtBytes(root?.used)) / \(fmtBytes($0))" })
            summaryTile("Up", primary: fmtUptime(latest?.uptime), secondary: nil)
        }
    }

    private func summaryTile(_ label: String, primary: String, secondary: String?) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(label).font(.caption).foregroundStyle(.secondary)
            Text(primary).font(.title3.bold())
            if let secondary {
                Text(secondary).font(.caption2).foregroundStyle(.secondary).lineLimit(1)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .fleetPanel()
    }

    @ViewBuilder
    private func sections(for host: Host, latest: Sample) -> some View {
        if let net = latest.net, !net.isEmpty {
            sectionPanel("Network") {
                ForEach(net, id: \.self) { n in
                    HStack {
                        Text(n.iface ?? "—").font(.subheadline)
                        Spacer()
                        Text("↓ \(fmtRate(n.rx_bps))").font(.caption).foregroundStyle(.secondary)
                        Text("↑ \(fmtRate(n.tx_bps))").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        if let disks = latest.disks, !disks.isEmpty {
            sectionPanel("Disks") {
                ForEach(disks, id: \.self) { d in
                    HStack {
                        Text(d.mount ?? d.device ?? "—").font(.subheadline)
                        Spacer()
                        Text("\(d.pct.map { Int($0) } ?? 0)%")
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(Theme.pctColor(d.pct))
                        Text("\(fmtBytes(d.used)) / \(fmtBytes(d.total))").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        if let gpus = latest.gpu, !gpus.isEmpty {
            sectionPanel("GPUs") {
                ForEach(gpus, id: \.self) { g in
                    VStack(alignment: .leading, spacing: 2) {
                        Text(g.name ?? g.vendor ?? "GPU").font(.subheadline)
                        HStack {
                            Text("\(g.util_pct.map { Int($0) } ?? 0)% util").font(.caption)
                            Spacer()
                            Text("\(g.temp.map { Int($0) } ?? 0)°C").font(.caption)
                            Spacer()
                            Text(g.power_w.map { "\(Int($0)) W" } ?? "—").font(.caption)
                        }
                        .foregroundStyle(.secondary)
                    }
                }
            }
        }
        if let bat = latest.battery, bat.present == true {
            sectionPanel("Battery") {
                HStack {
                    Text("\(bat.pct.map { Int($0) } ?? 0)%").font(.subheadline.bold())
                    Spacer()
                    Text(bat.ac_online == true ? "AC connected" : "on battery").font(.caption)
                    if let w = bat.wattage {
                        Text("\(String(format: "%.1f", w)) W").font(.caption).foregroundStyle(.secondary)
                    }
                }
            }
        }
        if let pools = latest.zfs_pools, !pools.isEmpty {
            sectionPanel("ZFS pools") {
                ForEach(pools, id: \.self) { p in
                    HStack {
                        Text(p.name ?? "—").font(.subheadline)
                        Spacer()
                        Text(p.state ?? "?")
                            .font(.caption.bold())
                            .foregroundStyle(p.state?.uppercased() == "ONLINE" ? Theme.ok : Theme.crit)
                        Text("\(p.cap ?? 0)%").font(.caption)
                    }
                }
            }
        }
        if let smart = latest.smart, !smart.isEmpty {
            sectionPanel("SMART") {
                ForEach(smart, id: \.self) { s in
                    HStack {
                        Text(s.device ?? "—").font(.subheadline)
                        Spacer()
                        Text(s.health ?? "?")
                            .font(.caption.bold())
                            .foregroundStyle(s.health == "PASSED" ? Theme.ok : Theme.crit)
                        if let t = s.temp { Text("\(Int(t))°C").font(.caption) }
                    }
                }
            }
        }
        if let svc = latest.services, !svc.isEmpty {
            sectionPanel("Services") {
                ForEach(Array(svc.prefix(30)), id: \.self) { s in
                    HStack {
                        Text(s.name ?? "—").font(.caption)
                        Spacer()
                        Text(s.status ?? "?")
                            .font(.caption.bold())
                            .foregroundStyle(s.status == "active" ? Theme.ok : (s.status == "failed" ? Theme.crit : Theme.warn))
                    }
                }
            }
        }
        if let containers = latest.containers, !containers.isEmpty {
            sectionPanel("Containers") {
                ForEach(containers, id: \.self) { c in
                    HStack {
                        Text(c.name ?? "—").font(.caption)
                        Spacer()
                        Text(c.status ?? "—").font(.caption2).foregroundStyle(.secondary).lineLimit(1)
                    }
                }
            }
        }
        if let logs = latest.logs, !logs.isEmpty {
            sectionPanel("Recent log errors") {
                ForEach(Array(logs.prefix(20)), id: \.self) { l in
                    VStack(alignment: .leading, spacing: 2) {
                        Text(l.unit ?? "—").font(.caption2.bold()).foregroundStyle(.secondary)
                        Text(l.message ?? "").font(.caption2).lineLimit(2)
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func sectionPanel<Content: View>(_ title: String, @ViewBuilder content: () -> Content) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(title)
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
            content()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .fleetPanel()
    }

    @ViewBuilder
    private func historySection(for host: Host) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text("History").font(.caption.weight(.semibold)).foregroundStyle(.secondary).textCase(.uppercase)
                Spacer()
                Picker("Window", selection: $window) {
                    Text("1h").tag("hour")
                    Text("24h").tag("day")
                    Text("7d").tag("week")
                }
                .pickerStyle(.segmented)
                .frame(width: 180)
            }
            if let err = historyError {
                Text(err).font(.caption).foregroundStyle(Theme.crit)
            }
            if let h = history {
                let interesting = ["cpu.pct", "mem.pct", "disk_root.pct", "cpu.temp", "gpu.temp_max", "net.rx_bps", "net.tx_bps"]
                ForEach(interesting.filter { h.metrics.contains($0) }, id: \.self) { metric in
                    historyChart(metric: metric, points: h.points)
                }
            } else {
                ProgressView().padding(.vertical, 30).frame(maxWidth: .infinity)
            }
        }
        .padding(.top, 8)
    }

    private func historyChart(metric: String, points: [HistoryPoint]) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(metric).font(.caption2).foregroundStyle(.secondary)
            Chart {
                ForEach(points, id: \.ts) { p in
                    if let v = p.values[metric] ?? nil {
                        LineMark(
                            x: .value("t", Date(timeIntervalSince1970: TimeInterval(p.ts))),
                            y: .value(metric, v)
                        )
                        .foregroundStyle(Theme.accent)
                    }
                }
            }
            .frame(height: 110)
            .chartXAxis { AxisMarks(values: .stride(by: .hour, count: 6)) { _ in
                AxisGridLine().foregroundStyle(Theme.line)
                AxisValueLabel().foregroundStyle(.secondary)
            }}
            .chartYAxis { AxisMarks { _ in
                AxisGridLine().foregroundStyle(Theme.line)
                AxisValueLabel().foregroundStyle(.secondary)
            }}
        }
        .fleetPanel()
    }

    private func loadHistory() async {
        do {
            history = try await fleet.fetchHistory(hostID: hostID, window: window)
            historyError = nil
        } catch {
            historyError = String(describing: error)
        }
    }
}
