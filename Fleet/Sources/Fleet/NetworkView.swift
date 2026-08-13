import SwiftUI

struct NetworkView: View {
    @EnvironmentObject var store: ArgusStore

    var body: some View {
        PollingScroll(fetch: { try await store.network() }) { resp in
            SpeedtestPanel()
            let wan = resp.wan?.last
            PanelSection(title: "WAN", systemImage: "globe") {
                DotRow(up: wan?.up ?? false, name: "Internet uplink",
                       trailing: (wan?.ms).map { String(format: "%.0f ms", $0) } ?? "—",
                       trailingColor: (wan?.up ?? false) ? Theme.green : Theme.red)
            }
            let pings = (resp.pings ?? [:]).sorted { lhs, rhs in
                (lhs.value.up ? 0 : 1, lhs.key) < (rhs.value.up ? 0 : 1, rhs.key)
            }
            let down = pings.filter { !$0.value.up }.count
            PanelSection(title: "PING MONITOR", systemImage: "dot.radiowaves.left.and.right",
                         trailing: down > 0 ? "\(down) DOWN" : "\(pings.count) UP") {
                ForEach(pings, id: \.key) { name, p in
                    DotRow(up: p.up, name: name,
                           detail: p.active_ip,
                           trailing: p.up ? (p.ms.map { String(format: "%.2f ms", $0) } ?? "OK") : "DOWN \(Fmt.ago(p.since))",
                           trailingColor: p.up ? Theme.inkDim : Theme.red)
                }
            }
        }
    }
}
