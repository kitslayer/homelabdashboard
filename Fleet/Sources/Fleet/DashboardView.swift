import SwiftUI

struct DashboardView: View {
    @EnvironmentObject var fleet: FleetStore
    @State private var query: String = ""
    @State private var sortBy: SortBy = .status

    enum SortBy: String, CaseIterable, Identifiable {
        case status, name, lastSeen, cpu, memory
        var id: String { rawValue }
        var label: String {
            switch self {
            case .status: return "Status"
            case .name: return "Name"
            case .lastSeen: return "Last seen"
            case .cpu: return "CPU"
            case .memory: return "Memory"
            }
        }
    }

    var filtered: [Host] {
        var out = fleet.hosts
        if !query.isEmpty {
            let q = query.lowercased()
            out = out.filter { h in
                let pieces = [h.hostname, h.display_name, h.ip, h.tailscale_ip]
                    .compactMap { $0 } + (h.tags ?? [])
                return pieces.joined(separator: " ").lowercased().contains(q)
            }
        }
        switch sortBy {
        case .status: out.sort { ($0.up ? 0 : 1, -($0.last_seen ?? 0)) < ($1.up ? 0 : 1, -($1.last_seen ?? 0)) }
        case .name: out.sort { $0.displayName.localizedCaseInsensitiveCompare($1.displayName) == .orderedAscending }
        case .lastSeen: out.sort { ($0.last_seen ?? 0) > ($1.last_seen ?? 0) }
        case .cpu: out.sort { ($0.latest?.cpu?.pct ?? 0) > ($1.latest?.cpu?.pct ?? 0) }
        case .memory: out.sort { ($0.latest?.mem?.pct ?? 0) > ($1.latest?.mem?.pct ?? 0) }
        }
        return out
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                ScrollView {
                    LazyVStack(spacing: 12) {
                        summaryRow
                        ForEach(filtered) { host in
                            NavigationLink(value: host.id) {
                                HostCard(host: host)
                            }.buttonStyle(.plain)
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.bottom, 24)
                }
                .refreshable { await fleet.refresh() }
            }
            .navigationTitle("Hosts")
            .searchable(text: $query, prompt: "filter by name, tag, or IP")
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Menu {
                        Picker("Sort", selection: $sortBy) {
                            ForEach(SortBy.allCases) { Text($0.label).tag($0) }
                        }
                    } label: {
                        Image(systemName: "arrow.up.arrow.down.circle")
                    }
                }
            }
            .navigationDestination(for: Int.self) { hostID in
                HostDetailView(hostID: hostID)
            }
        }
    }

    private var summaryRow: some View {
        HStack(spacing: 8) {
            pill(value: fleet.hosts.count, label: "hosts")
            pill(value: fleet.hosts.filter(\.up).count, label: "up")
            pill(value: fleet.activeAlerts.count, label: "alerts", tint: fleet.activeAlerts.isEmpty ? .secondary : Theme.warn)
            Spacer()
            if let ts = fleet.lastUpdate {
                Text(relativeFormatter.localizedString(for: ts, relativeTo: Date()))
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.vertical, 4)
    }

    private func pill(value: Int, label: String, tint: Color = .primary) -> some View {
        HStack(spacing: 6) {
            Text("\(value)").font(.system(.body, design: .rounded).weight(.semibold)).foregroundStyle(tint)
            Text(label).font(.caption).foregroundStyle(.secondary)
        }
        .padding(.horizontal, 12).padding(.vertical, 6)
        .background(Capsule().fill(Theme.panel))
    }
}

let relativeFormatter: RelativeDateTimeFormatter = {
    let f = RelativeDateTimeFormatter()
    f.unitsStyle = .abbreviated
    return f
}()

struct HostCard: View {
    let host: Host

    var statusColor: Color {
        if !host.up { return Theme.crit }
        let cpu = host.latest?.cpu?.pct ?? 0
        let mem = host.latest?.mem?.pct ?? 0
        let temp = host.latest?.cpu?.temp ?? 0
        if cpu >= 95 || mem >= 95 || temp >= 90 { return Theme.warn }
        return Theme.ok
    }
    var statusText: String {
        guard host.last_seen != nil else { return "pending" }
        return host.up ? "up" : "down"
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(host.displayName).font(.headline)
                    Text([host.distro, host.kernel, host.ip].compactMap { $0 }.joined(separator: " · "))
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
                Spacer()
                Text(statusText.uppercased())
                    .font(.caption2.bold())
                    .padding(.horizontal, 8).padding(.vertical, 3)
                    .background(Capsule().fill(statusColor.opacity(0.18)))
                    .foregroundStyle(statusColor)
            }
            HStack(spacing: 10) {
                metricTile("CPU", value: host.latest?.cpu?.pct, kind: .percent, sub: host.latest?.cpu?.temp.map { "\(Int($0))°C" })
                metricTile("Mem", value: host.latest?.mem?.pct, kind: .percent, sub: nil)
                metricTile("Disk /", value: host.latest?.rootDisk?.pct, kind: .percent, sub: nil)
                metricTile("Uptime", value: nil, kind: .text(fmtUptime(host.latest?.uptime)), sub: nil)
            }
            if let tags = host.tags, !tags.isEmpty {
                HStack(spacing: 4) {
                    ForEach(tags, id: \.self) { t in
                        Text(t)
                            .font(.caption2)
                            .padding(.horizontal, 7).padding(.vertical, 2)
                            .background(Capsule().fill(Theme.line))
                    }
                }
            }
        }
        .fleetPanel()
    }

    enum MetricKind {
        case percent
        case text(String)
    }

    private func metricTile(_ label: String, value: Double?, kind: MetricKind, sub: String?) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label).font(.caption2).foregroundStyle(.secondary)
            switch kind {
            case .percent:
                Text(value.map { "\(Int($0))%" } ?? "—")
                    .font(.system(.subheadline, design: .rounded).weight(.semibold))
                    .foregroundStyle(Theme.pctColor(value))
            case .text(let str):
                Text(str)
                    .font(.system(.subheadline, design: .rounded).weight(.semibold))
            }
            if let sub {
                Text(sub).font(.caption2).foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(8)
        .background(RoundedRectangle(cornerRadius: 8).fill(Color.black.opacity(0.25)))
    }
}

func fmtUptime(_ seconds: Int?) -> String {
    guard let s = seconds else { return "—" }
    let d = s / 86400
    let h = (s % 86400) / 3600
    let m = (s % 3600) / 60
    if d > 0 { return "\(d)d \(h)h" }
    if h > 0 { return "\(h)h \(m)m" }
    return "\(m)m"
}

func fmtBytes(_ n: Int64?) -> String {
    guard let n else { return "—" }
    let units = ["B", "KB", "MB", "GB", "TB", "PB"]
    var v = Double(n)
    var i = 0
    while v >= 1024 && i < units.count - 1 { v /= 1024; i += 1 }
    return String(format: v >= 10 || i == 0 ? "%.0f %@" : "%.1f %@", v, units[i])
}

func fmtRate(_ n: Double?) -> String {
    guard let n else { return "—" }
    return fmtBytes(Int64(n)) + "/s"
}
