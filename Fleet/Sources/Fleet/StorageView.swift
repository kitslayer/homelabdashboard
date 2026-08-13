import SwiftUI

struct StorageView: View {
    @EnvironmentObject var store: ArgusStore

    var body: some View {
        PollingScroll(fetch: { try await store.storage() }, interval: 15) { resp in
            if let e = resp.err, !e.isEmpty { ErrPanel(message: e) }
            let pools = resp.truenas?.pools ?? []
            if !pools.isEmpty {
                PanelSection(title: "ZFS POOLS", systemImage: "externaldrive", trailing: "\(pools.count)") {
                    ForEach(pools) { p in
                        VStack(alignment: .leading, spacing: 6) {
                            HStack {
                                Text(p.name ?? "?").font(Theme.display(13)).tracking(0.5).foregroundStyle(Theme.ink)
                                Spacer()
                                StatusChip(status: p.health ?? "?")
                            }
                            Gauge(label: "CAP", pct: p.cap_pct, warn: 80, crit: 90)
                            if p.size != nil {
                                Text("\(p.alloc ?? "?") used of \(p.size ?? "?")  ·  \(p.free ?? "?") free")
                                    .font(Theme.mono(10)).foregroundStyle(Theme.inkFaint)
                            }
                        }
                        .padding(.vertical, 8)
                        .overlay(alignment: .bottom) { Rectangle().fill(Theme.line).frame(height: 1) }
                    }
                }
            }
            let ds = resp.truenas?.datasets ?? []
            if !ds.isEmpty {
                PanelSection(title: "DATASETS", systemImage: "folder", trailing: "\(ds.count)") {
                    ForEach(ds.prefix(50)) { d in
                        DotRow(up: nil, name: d.name ?? "?", trailing: d.used ?? "")
                    }
                }
            }
            if pools.isEmpty && ds.isEmpty && (resp.err ?? "").isEmpty {
                PanelSection(title: "STORAGE", systemImage: "externaldrive") {
                    Text("No storage data reported.").font(Theme.mono(12)).foregroundStyle(Theme.inkFaint)
                }
            }
        }
    }
}
