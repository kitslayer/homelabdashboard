import SwiftUI

struct K8sView: View {
    @EnvironmentObject var store: ArgusStore
    @State private var pod: K8sPod?
    @State private var workload: K8sWorkload?

    var body: some View {
        PollingScroll(fetch: { try await store.k8s() }) { resp in
            if let e = resp.err, !e.isEmpty { ErrPanel(message: e) }
            K8sEventsPanel()
            if let nodes = resp.state?.nodes, !nodes.isEmpty {
                PanelSection(title: "NODES", systemImage: "cpu",
                             trailing: "\(nodes.filter { $0.ready }.count)/\(nodes.count) READY") {
                    ForEach(nodes) { n in
                        DotRow(up: n.ready, name: n.name, detail: n.ip, trailing: n.version)
                    }
                }
            }
            if let wls = resp.state?.workloads, !wls.isEmpty {
                PanelSection(title: "WORKLOADS", systemImage: "square.stack.3d.up", trailing: "\(wls.count)") {
                    ForEach(wls.sorted { (($0.ready ?? 0) == ($0.desired ?? 0) ? 1 : 0, $0.name) < (($1.ready ?? 0) == ($1.desired ?? 0) ? 1 : 0, $1.name) }) { w in
                        Button { workload = w } label: {
                            DotRow(up: (w.ready ?? 0) == (w.desired ?? 0) && (w.desired ?? 0) > 0,
                                   name: w.name, detail: "\(w.kind ?? "") · \(w.ns ?? "")",
                                   trailing: "\(w.ready ?? 0)/\(w.desired ?? 0)",
                                   trailingColor: (w.ready ?? 0) == (w.desired ?? 0) ? Theme.inkDim : Theme.amber)
                        }.buttonStyle(.plain)
                    }
                }
            }
            if let pods = resp.state?.pods, !pods.isEmpty {
                let byNS = Dictionary(grouping: pods) { $0.ns ?? "default" }
                ForEach(byNS.keys.sorted(), id: \.self) { ns in
                    let list = (byNS[ns] ?? []).sorted {
                        ($0.phase == "Running" ? 0 : 1, $0.name) < ($1.phase == "Running" ? 0 : 1, $1.name)
                    }
                    let bad = list.filter { $0.phase != "Running" && $0.phase != "Succeeded" }.count
                    PanelSection(title: ns, systemImage: "circle.hexagongrid",
                                 trailing: bad > 0 ? "\(bad) BAD" : "\(list.count)") {
                        ForEach(list) { p in
                            Button { pod = p } label: {
                                DotRow(up: p.phase == "Running" || p.phase == "Succeeded", name: p.name,
                                       detail: "\(p.node ?? "")  ·  \(p.ready ?? "")",
                                       trailing: (p.restarts ?? 0) > 0 ? "↻\(p.restarts!)" : (p.phase ?? ""),
                                       trailingColor: (p.restarts ?? 0) > 3 ? Theme.amber : Theme.inkDim)
                            }.buttonStyle(.plain)
                        }
                    }
                }
            }
        }
        .sheet(item: $pod) { p in
            ActionSheet2(title: p.name, subtitle: "\(p.ns ?? "")  ·  \(p.node ?? "")",
                         status: p.phase ?? "?",
                         actions: [
                            PowerAction(label: "DELETE POD (RESTART)", destructive: true, ms: 1100) {
                                try await store.k8sDeletePod(ns: p.ns ?? "default", name: p.name)
                            }
                         ],
                         logs: { try await store.k8sLogs(ns: p.ns ?? "default", pod: p.name) })
        }
        .sheet(item: $workload) { w in
            ActionSheet2(title: w.name, subtitle: "\(w.kind ?? "") · \(w.ns ?? "")",
                         status: (w.ready ?? 0) == (w.desired ?? 0) ? "running" : "degraded",
                         actions: [
                            PowerAction(label: "ROLLOUT RESTART", destructive: true, ms: 1000) {
                                try await store.k8sRestart(kind: w.kind ?? "deployment", ns: w.ns ?? "default", name: w.name)
                            }
                         ])
        }
    }
}
