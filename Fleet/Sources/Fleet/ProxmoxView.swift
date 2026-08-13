import SwiftUI

struct ProxmoxView: View {
    @EnvironmentObject var store: ArgusStore
    @State private var vm: PVEVM?

    var body: some View {
        PollingScroll(fetch: { try await store.pve() }) { resp in
            if let e = resp.err, !e.isEmpty { ErrPanel(message: e) }
            if let nodes = resp.state?.nodes, !nodes.isEmpty {
                PanelSection(title: "NODES", systemImage: "server.rack", trailing: "\(nodes.count)") {
                    VStack(spacing: 12) { ForEach(nodes) { nodeCard($0) } }
                }
            }
            if let vms = resp.state?.vms, !vms.isEmpty {
                let byNode = Dictionary(grouping: vms) { $0.node ?? "?" }
                ForEach(byNode.keys.sorted(), id: \.self) { node in
                    let list = (byNode[node] ?? []).sorted {
                        ($0.status == "running" ? 0 : 1, $0.vmid) < ($1.status == "running" ? 0 : 1, $1.vmid)
                    }
                    PanelSection(title: node, systemImage: "cube.transparent", trailing: "\(list.count) VM") {
                        ForEach(list) { v in
                            Button { vm = v } label: {
                                DotRow(up: v.status == "running", name: v.name ?? "vm \(v.vmid)",
                                       detail: "vmid \(v.vmid) · \(v.type ?? "")",
                                       trailing: (v.lock ?? v.status ?? "").uppercased(),
                                       trailingColor: v.status == "running" ? Theme.green : Theme.inkFaint)
                            }.buttonStyle(.plain)
                        }
                    }
                }
            }
        }
        .sheet(item: $vm) { v in
            ActionSheet2(title: v.name ?? "VM \(v.vmid)",
                         subtitle: "vmid \(v.vmid) · \(v.node ?? "")",
                         status: v.status ?? "unknown",
                         actions: vmActions(v))
        }
    }

    private func nodeCard(_ n: PVENode) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(n.node).font(Theme.display(14)).tracking(1).foregroundStyle(Theme.ink)
                Spacer()
                StatusChip(status: n.status ?? "?")
                if let u = n.uptime { Text(Fmt.uptime(u)).font(Theme.mono(10)).foregroundStyle(Theme.inkFaint) }
            }
            Gauge(label: "CPU", pct: n.cpuPct)
            Gauge(label: "MEM", pct: n.memPct)
            Gauge(label: "DISK", pct: n.diskPct, warn: 80, crit: 92)
        }
        .padding(12)
        .background(Theme.bgRaise)
        .overlay(Rectangle().stroke(Theme.line, lineWidth: 1))
    }

    private func vmActions(_ v: PVEVM) -> [PowerAction] {
        if v.status == "running" {
            return [
                PowerAction(label: "REBOOT", destructive: true) { try await store.pvePower(vmid: v.vmid, op: "reboot") },
                PowerAction(label: "SHUTDOWN", destructive: true) { try await store.pvePower(vmid: v.vmid, op: "shutdown") },
                PowerAction(label: "STOP (HARD)", destructive: true, ms: 1400) { try await store.pvePower(vmid: v.vmid, op: "stop") },
            ]
        }
        return [ PowerAction(label: "START", destructive: false) { try await store.pvePower(vmid: v.vmid, op: "start") } ]
    }
}
