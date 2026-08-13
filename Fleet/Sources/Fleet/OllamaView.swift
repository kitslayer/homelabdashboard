import SwiftUI

struct OllamaView: View {
    @EnvironmentObject var store: ArgusStore
    @State private var unload: UnloadSel?
    @State private var chatSel: ChatSel?

    struct UnloadSel: Identifiable { var target: String; var model: String; var id: String { target + "/" + model } }
    struct ChatSel: Identifiable { var target: String; var model: String; var id: String { target + "/" + model } }

    var body: some View {
        PollingScroll(fetch: { try await store.ollama() }, interval: 8) { resp in
            ForEach(resp.targets ?? []) { t in
                let psModels = t.ps?.models ?? []
                let loaded = Set(psModels.map { $0.name })
                PanelSection(title: t.name, systemImage: "cpu",
                             trailing: (t.err?.isEmpty == false) ? "OFFLINE" : "\(t.tags?.models?.count ?? 0) MODELS") {
                    if let e = t.err, !e.isEmpty {
                        Text(e).font(Theme.mono(10)).foregroundStyle(Theme.red)
                    }
                    if !psModels.isEmpty {
                        Text("LOADED IN VRAM").font(Theme.display(9)).tracking(2).foregroundStyle(Theme.amber).padding(.top, 2)
                        ForEach(psModels) { m in
                            Button { unload = UnloadSel(target: t.id, model: m.name) } label: {
                                DotRow(up: true, name: m.name, detail: "tap to unload",
                                       trailing: Fmt.bytes(m.size_vram ?? m.size), trailingColor: Theme.amber)
                            }.buttonStyle(.plain)
                        }
                    }
                    if let models = t.tags?.models, !models.isEmpty {
                        Text("AVAILABLE — TAP TO CHAT").font(Theme.display(9)).tracking(2).foregroundStyle(Theme.inkFaint).padding(.top, 6)
                        ForEach(models) { m in
                            Button { chatSel = ChatSel(target: t.id, model: m.name) } label: {
                                DotRow(up: loaded.contains(m.name) ? true : nil, name: m.name,
                                       detail: [m.details?.parameter_size, m.details?.quantization_level].compactMap { $0 }.joined(separator: " · "),
                                       trailing: Fmt.bytes(m.size))
                            }.buttonStyle(.plain)
                        }
                    }
                }
            }
        }
        .sheet(item: $unload) { u in
            ActionSheet2(title: u.model, subtitle: "unload from VRAM", status: "loaded",
                         actions: [ PowerAction(label: "UNLOAD", destructive: true) {
                             try await store.ollamaUnload(target: u.target, model: u.model)
                         } ])
        }
        .sheet(item: $chatSel) { c in OllamaChatView(target: c.target, model: c.model) }
    }
}
