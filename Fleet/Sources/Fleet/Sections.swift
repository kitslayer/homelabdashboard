import SwiftUI

// ════════════════════════════════════════════════════════════════════
//  Section views. Hosts is real; others are styled stubs (next phase).
// ════════════════════════════════════════════════════════════════════

// MARK: - Hosts (grouped full tiles)

struct HostsView: View {
    @EnvironmentObject var store: ArgusStore
    @State private var selected: HostRef?
    @State private var query = ""
    private let cols = [GridItem(.flexible(), spacing: 12), GridItem(.flexible(), spacing: 12)]

    var body: some View {
        ScrollView {
            VStack(spacing: 12) {
                searchField
                ForEach(HostGroup.order, id: \.self) { g in
                    let hosts = store.hosts.filter { $0.groupKey == g && matches($0) }
                    if !hosts.isEmpty {
                        GroupLabel(text: HostGroup.name(g))
                        LazyVGrid(columns: cols, spacing: 12) {
                            ForEach(hosts) { h in
                                Button { selected = HostRef(id: h.name) } label: { HostTile(host: h) }.buttonStyle(.plain)
                            }
                        }
                    }
                }
            }
            .padding(.horizontal, 16).padding(.vertical, 14)
        }
        .refreshable { await store.refresh() }
        .sheet(item: $selected) { ref in HostDetailView(name: ref.id) }
    }

    private func matches(_ h: HostView) -> Bool {
        guard !query.isEmpty else { return true }
        let q = query.lowercased()
        return [h.name, h.label, h.ip, h.group, h.fleet?.distro].compactMap { $0 }.joined(separator: " ").lowercased().contains(q)
    }

    private var searchField: some View {
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass").font(.system(size: 12)).foregroundStyle(Theme.inkFaint)
            TextField("filter hosts", text: $query)
                .font(Theme.mono(13)).foregroundStyle(Theme.ink).autocorrectionDisabled()
                #if os(iOS)
                .textInputAutocapitalization(.never)
                #endif
            if !query.isEmpty {
                Button { query = "" } label: { Image(systemName: "xmark.circle.fill").font(.system(size: 13)).foregroundStyle(Theme.inkFaint) }.buttonStyle(.plain)
            }
        }
        .padding(.horizontal, 10).padding(.vertical, 9)
        .background(Theme.panel).clipShape(Chamfer(cut: 8)).overlay(Chamfer(cut: 8).stroke(Theme.line, lineWidth: 1))
    }
}

// MARK: - Stubs (to be built out)

private struct StubView: View {
    var title: String
    var systemImage: String
    var note: String = "Coming online in the next build phase."

    var body: some View {
        VStack {
            Spacer()
            VStack(spacing: 14) {
                Image(systemName: systemImage).font(.system(size: 40)).foregroundStyle(Theme.amber.opacity(0.7))
                Text(title).font(Theme.display(16)).tracking(3).foregroundStyle(Theme.inkDim)
                Text(note).font(Theme.mono(12)).foregroundStyle(Theme.inkFaint)
                    .multilineTextAlignment(.center)
            }
            .argusPanel(24)
            .padding(.horizontal, 30)
            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// HermesView, and all other section views, are implemented in their own files now.

// MARK: - Settings (Argus server + status)

struct SettingsView: View {
    @EnvironmentObject var store: ArgusStore
    @Environment(\.dismiss) private var dismiss
    @State private var draft: String = ""

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                ScrollView {
                    VStack(alignment: .leading, spacing: 14) {
                        VStack(alignment: .leading, spacing: 10) {
                            PanelTitle(text: "ARGUS SERVER", systemImage: "server.rack")
                            TextField("http://192.168.1.195:8800", text: $draft)
                                .font(Theme.mono(14)).foregroundStyle(Theme.ink)
                                .autocorrectionDisabled()
                                #if os(iOS)
                                .textInputAutocapitalization(.never)
                                .keyboardType(.URL)
                                #endif
                                .padding(10)
                                .background(Theme.bgRaise)
                                .overlay(Rectangle().stroke(Theme.edgeHard, lineWidth: 1))
                            HStack {
                                Text("stored on this device").font(Theme.mono(10)).foregroundStyle(Theme.inkFaint)
                                Spacer()
                                Button { store.base = draft.trimmingCharacters(in: .whitespaces); Task { await store.refresh() } } label: {
                                    Text("SAVE").font(Theme.display(12)).tracking(1.4).foregroundStyle(Theme.amber)
                                        .padding(.horizontal, 16).padding(.vertical, 10)
                                        .overlay(Chamfer(cut: 8).stroke(Theme.edgeHard, lineWidth: 1))
                                }.buttonStyle(.plain)
                            }
                        }
                        .argusPanel()

                        VStack(alignment: .leading, spacing: 8) {
                            PanelTitle(text: "STATUS")
                            kv("CONNECTION", store.lastError == nil ? "ONLINE" : "ERROR",
                               color: store.lastError == nil ? Theme.green : Theme.red)
                            if let e = store.lastError { kv("LAST ERROR", e, color: Theme.red) }
                            kv("ARGUS VERSION", store.uiConfig?.version ?? "—")
                            if let u = store.lastUpdate {
                                kv("LAST UPDATE", Fmt.ago(Int(u.timeIntervalSince1970)))
                            }
                            kv("HOSTS", "\(store.hosts.count)")
                        }
                        .argusPanel()
                        ActivityPanel()
                        Spacer()
                    }
                    .padding(16)
                }
            }
            .navigationTitle("SETTINGS")
            .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Done") { dismiss() } } }
            .onAppear { draft = store.base }
        }
        .preferredColorScheme(.dark)
    }

    private func kv(_ k: String, _ v: String, color: Color = Theme.ink) -> some View {
        HStack {
            Text(k).font(Theme.display(10)).tracking(1.6).foregroundStyle(Theme.inkFaint)
            Spacer()
            Text(v).font(Theme.mono(12)).foregroundStyle(color).multilineTextAlignment(.trailing)
        }
        .padding(.vertical, 5)
    }
}
