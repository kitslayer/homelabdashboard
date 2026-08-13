import SwiftUI

struct ServicesView: View {
    @EnvironmentObject var store: ArgusStore
    @State private var sel: Service?

    var body: some View {
        PollingScroll(fetch: { try await store.services() }) { resp in
            if let mc = resp.minecraft, !mc.isEmpty {
                PanelSection(title: "MINECRAFT", systemImage: "cube.fill") {
                    ForEach(mc) { s in
                        DotRow(up: s.up, name: s.name,
                               detail: [s.motd, s.version].compactMap { $0 }.joined(separator: " · "),
                               trailing: s.up ? "\(s.online ?? 0)/\(s.max ?? 0)" : "DOWN",
                               trailingColor: s.up ? Theme.ink : Theme.red)
                    }
                }
            }
            let groups = Dictionary(grouping: resp.services ?? []) { $0.group ?? "Other" }
            ForEach(groups.keys.sorted(), id: \.self) { g in
                let svcs = (groups[g] ?? []).sorted { ($0.up ? 0 : 1, $0.name.lowercased()) < ($1.up ? 0 : 1, $1.name.lowercased()) }
                let down = svcs.filter { !$0.up }.count
                PanelSection(title: g, trailing: down > 0 ? "\(down) DOWN" : "\(svcs.count)") {
                    ForEach(svcs) { s in
                        Button { sel = s } label: {
                            DotRow(up: s.up, name: s.name, detail: s.url,
                                   trailing: s.up ? (s.ms.map { String(format: "%.0fms", $0) } ?? "OK") : "DOWN \(Fmt.ago(s.since))",
                                   trailingColor: s.up ? Theme.inkDim : Theme.red)
                        }.buttonStyle(.plain)
                    }
                }
            }
        }
        .sheet(item: $sel) { ServiceDetailSheet(s: $0) }
    }
}

struct ServiceDetailSheet: View {
    var s: Service
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                ScrollView {
                    VStack(spacing: 12) {
                        PanelSection(title: "STATUS") {
                            HStack {
                                Text(s.group ?? "").font(Theme.mono(12)).foregroundStyle(Theme.inkDim)
                                Spacer()
                                StatusChip(s.up ? "UP" : "DOWN", color: s.up ? Theme.green : Theme.red)
                            }
                        }
                        PanelSection(title: "DETAILS") {
                            kv("URL", s.url ?? "—")
                            kv("HTTP", s.code.map { String($0) } ?? "—")
                            kv("LATENCY", s.ms.map { String(format: "%.1f ms", $0) } ?? "—")
                            if let ok = s.ok_count, let tot = s.total, tot > 0 {
                                kv("UPTIME", String(format: "%.2f%%", Double(ok) / Double(tot) * 100) + "  (\(ok)/\(tot))")
                            }
                            kv("FAILS IN ROW", "\(s.fails_in_row ?? 0)")
                            if let since = s.since { kv("STABLE SINCE", Fmt.ago(since)) }
                        }
                        Spacer()
                    }
                    .padding(16)
                }
            }
            .navigationTitle(s.name)
            .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Close") { dismiss() } } }
        }
        .preferredColorScheme(.dark)
    }

    private func kv(_ k: String, _ v: String) -> some View {
        HStack(alignment: .top) {
            Text(k).font(Theme.display(10)).tracking(1.4).foregroundStyle(Theme.inkFaint)
            Spacer()
            Text(v).font(Theme.mono(12)).foregroundStyle(Theme.ink).multilineTextAlignment(.trailing)
        }
        .padding(.vertical, 5)
        .overlay(alignment: .bottom) { Rectangle().fill(Theme.line).frame(height: 1) }
    }
}
