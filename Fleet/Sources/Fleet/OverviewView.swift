import SwiftUI

struct OverviewView: View {
    @EnvironmentObject var store: ArgusStore
    var go: (Tab) -> Void
    @State private var selected: HostRef?

    private let cols = [GridItem(.flexible(), spacing: 12), GridItem(.flexible(), spacing: 12)]

    var body: some View {
        ScrollView {
            VStack(spacing: 14) {
                hero
                countCards
                alertsPanel
                hostGroups
                footer
            }
            .padding(.horizontal, 16).padding(.vertical, 14)
        }
        .refreshable { await store.refresh() }
        .sheet(item: $selected) { ref in HostDetailView(name: ref.id) }
    }

    // ── hero: greeting / clock / weather / status ────────────────────
    private var hero: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text(greeting).font(Theme.display(12)).tracking(3.4).foregroundStyle(Theme.inkFaint)
            TimelineView(.periodic(from: .now, by: 1)) { ctx in
                Text(hm(ctx.date)).font(Theme.mono(54, .medium)).foregroundStyle(Theme.amber)
                Text(dateLine(ctx.date)).font(Theme.display(12)).tracking(2.5).foregroundStyle(Theme.inkDim)
            }
            if let wx = store.overview?.weather, let t = wx.temp_c {
                HStack(spacing: 6) {
                    Text(String(format: "%.0f°C", t)).font(Theme.mono(15)).foregroundStyle(Theme.amber)
                    Text(wx.desc ?? "").font(Theme.mono(13)).foregroundStyle(Theme.inkDim)
                    if let hi = wx.today_max_c, let lo = wx.today_min_c {
                        Text(String(format: "↑%.0f° ↓%.0f°", hi, lo)).font(Theme.mono(12)).foregroundStyle(Theme.inkFaint)
                    }
                }
            }
            statusLine.padding(.top, 4)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .argusPanel(20)
    }

    private var statusLine: some View {
        Group {
            if store.hasCritical {
                Text("▲ ATTENTION REQUIRED").foregroundStyle(Theme.red)
            } else if store.alertCount > 0 {
                Text("● DEGRADED — \(store.alertCount) OPEN").foregroundStyle(Theme.amber)
            } else {
                Text("● ALL SYSTEMS NOMINAL").foregroundStyle(Theme.green)
            }
        }
        .font(Theme.display(14)).tracking(1.5)
    }

    // ── count cards ──────────────────────────────────────────────────
    private var countCards: some View {
        let ov = store.overview
        let hosts = ov?.hosts ?? []
        let up = hosts.filter(\.up).count
        let svc = ov?.services ?? Count()
        let k3s = ov?.k3s
        let dock = (ov?.docker ?? [:]).values.reduce((0, 0)) { ($0.0 + $1.up, $0.1 + $1.total) }
        let pools = ov?.pools ?? []
        let poolsBad = pools.filter { $0.health != "ONLINE" }.count

        return LazyVGrid(columns: cols, spacing: 12) {
            Button { go(.hosts) } label: {
                CountCard(big: "\(up)/\(hosts.count)", label: "HOSTS UP",
                          color: up == hosts.count ? Theme.green : Theme.amber) }.buttonStyle(.plain)
            Button { go(.services) } label: {
                CountCard(big: "\(svc.up)/\(svc.total)", label: "SERVICES",
                          color: svc.up == svc.total ? Theme.green : Theme.amber) }.buttonStyle(.plain)
            Button { go(.k8s) } label: {
                CountCard(big: "\(k3s?.nodes_ready ?? 0)/\(k3s?.nodes_total ?? 0)",
                          label: "K3S · \(k3s?.pods_running ?? 0) PODS",
                          color: (k3s?.pods_bad ?? 0) == 0 ? Theme.green : Theme.amber) }.buttonStyle(.plain)
            Button { go(.docker) } label: {
                CountCard(big: "\(dock.0)/\(dock.1)", label: "CONTAINERS", color: Theme.green) }.buttonStyle(.plain)
            Button { go(.storage) } label: {
                CountCard(big: pools.isEmpty ? "—" : "\(pools.count - poolsBad)/\(pools.count)",
                          label: "ZFS POOLS", color: poolsBad > 0 ? Theme.red : Theme.green) }.buttonStyle(.plain)
        }
    }

    // ── active alerts ────────────────────────────────────────────────
    private var alertsPanel: some View {
        let synth = store.overview?.alerts?.synthetic ?? []
        let fleet = store.overview?.alerts?.fleet ?? []
        let total = synth.count + fleet.count
        return VStack(alignment: .leading, spacing: 8) {
            PanelTitle(text: "ACTIVE ALERTS", systemImage: "exclamationmark.triangle") {
                Text(total > 0 ? "\(total) OPEN" : "")
            }
            if total == 0 {
                Text("▣ ALL SYSTEMS NOMINAL").font(Theme.display(14)).tracking(1.4).foregroundStyle(Theme.green)
                    .padding(.vertical, 6)
            } else {
                ForEach(synth) { a in
                    if let t = mapTab(a.tab) {
                        Button { go(t) } label: { alertRow(sev: a.severity, msg: a.message, when: nil) }.buttonStyle(.plain)
                    } else {
                        alertRow(sev: a.severity, msg: a.message, when: nil)
                    }
                }
                ForEach(fleet) { a in alertRow(sev: a.severity, msg: a.message, when: Fmt.ago(a.fired_at), n: a.count) }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .argusPanel()
    }

    private func alertRow(sev: String, msg: String, when: String?, n: Int? = nil) -> some View {
        HStack(alignment: .top, spacing: 10) {
            Text(sev.uppercased()).font(Theme.display(9.5)).tracking(1.6)
                .foregroundStyle(Theme.severity(sev)).frame(width: 64, alignment: .leading)
            Text(msg + ((n ?? 1) > 1 ? "  ×\(n!)" : "")).font(Theme.mono(12)).foregroundStyle(Theme.ink)
            Spacer(minLength: 4)
            if let when, !when.isEmpty {
                Text(when).font(Theme.mono(10)).foregroundStyle(Theme.inkFaint)
            }
        }
        .padding(.vertical, 7).padding(.horizontal, 10)
        .background(Theme.bgRaise)
        .overlay(alignment: .leading) {
            Rectangle().fill(Theme.severity(sev)).frame(width: 3)
        }
    }

    // ── grouped host tiles ───────────────────────────────────────────
    private var hostGroups: some View {
        ForEach(HostGroup.order, id: \.self) { g in
            let hosts = store.hosts.filter { $0.groupKey == g }
            if !hosts.isEmpty {
                GroupLabel(text: HostGroup.name(g))
                LazyVGrid(columns: cols, spacing: 12) {
                    ForEach(hosts) { h in
                        Button { selected = HostRef(id: h.name) } label: { HostTile(host: h, compact: true) }.buttonStyle(.plain)
                    }
                }
            }
        }
    }

    // ── helpers ──────────────────────────────────────────────────────
    private var footer: some View {
        HStack(spacing: 5) {
            if let u = store.lastUpdate { Text("updated \(Fmt.ago(Int(u.timeIntervalSince1970)))") }
            if let v = store.uiConfig?.version { Text("· argus \(v)") }
        }
        .font(Theme.mono(9.5)).foregroundStyle(Theme.inkFaint)
        .frame(maxWidth: .infinity).padding(.top, 6).padding(.bottom, 2)
    }

    private func mapTab(_ s: String?) -> Tab? {
        guard let s else { return nil }
        let seg = s.replacingOccurrences(of: "#/", with: "").split(separator: "/").first.map(String.init) ?? ""
        return Tab(rawValue: seg)
    }

    private var greeting: String {
        let hr = Calendar.current.component(.hour, from: Date())
        if hr < 5 { return "WORKING LATE, SIR?" }
        if hr < 12 { return "GOOD MORNING" }
        if hr < 18 { return "GOOD AFTERNOON" }
        return "GOOD EVENING"
    }
    private func hm(_ d: Date) -> String { let f = DateFormatter(); f.dateFormat = "HH:mm"; return f.string(from: d) }
    private func dateLine(_ d: Date) -> String {
        let f = DateFormatter(); f.dateFormat = "EEEE, MMMM d"; return f.string(from: d).uppercased()
    }
}
