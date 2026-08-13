import SwiftUI

struct DockerView: View {
    @EnvironmentObject var store: ArgusStore
    @State private var sel: ContainerSel?

    struct ContainerSel: Identifiable {
        var target: String
        var targetName: String
        var c: DockerContainer
        var id: String { target + "/" + c.id }
    }

    var body: some View {
        PollingScroll(fetch: { try await store.docker() }) { resp in
            if let e = resp.err, !e.isEmpty { ErrPanel(message: e) }
            ForEach(resp.targets ?? []) { t in
                let list = (resp.containers?[t.id] ?? []).sorted {
                    ($0.state == "running" ? 0 : 1, $0.name) < ($1.state == "running" ? 0 : 1, $1.name)
                }
                let up = list.filter { $0.state == "running" }.count
                PanelSection(title: t.name, systemImage: "shippingbox", trailing: "\(up)/\(list.count)") {
                    ForEach(list) { c in
                        Button { sel = ContainerSel(target: t.id, targetName: t.name, c: c) } label: {
                            DotRow(up: c.state == "running", name: c.name, detail: c.image,
                                   trailing: c.status, trailingColor: Theme.inkFaint)
                        }.buttonStyle(.plain)
                    }
                }
            }
        }
        .sheet(item: $sel) { s in
            ActionSheet2(title: s.c.name, subtitle: s.targetName + " · " + (s.c.image ?? ""),
                         status: s.c.state ?? "?", actions: actions(s),
                         logs: { try await store.dockerLogs(target: s.target, name: s.c.name) },
                         statsLine: statsLine(s))
        }
    }

    private func actions(_ s: ContainerSel) -> [PowerAction] {
        if s.c.state == "running" {
            return [
                PowerAction(label: "RESTART", destructive: true) { try await store.dockerAction(target: s.target, name: s.c.name, op: "restart") },
                PowerAction(label: "STOP", destructive: true) { try await store.dockerAction(target: s.target, name: s.c.name, op: "stop") },
            ]
        }
        return [ PowerAction(label: "START", destructive: false) { try await store.dockerAction(target: s.target, name: s.c.name, op: "start") } ]
    }

    private func statsLine(_ s: ContainerSel) -> (@MainActor () async throws -> String)? {
        guard s.c.state == "running" else { return nil }
        let target = s.target, name = s.c.name
        return {
            let st = try await store.dockerStats(target: target, name: name)
            return "CPU \(String(format: "%.1f", st.cpu_pct ?? 0))%   ·   MEM \(st.mem_display ?? "—")"
        }
    }
}
