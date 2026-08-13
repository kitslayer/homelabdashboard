import SwiftUI

struct OllamaChatView: View {
    @EnvironmentObject var store: ArgusStore
    var target: String
    var model: String
    @Environment(\.dismiss) private var dismiss

    @State private var messages: [ChatMsg] = []
    @State private var input = ""
    @State private var busy = false

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                VStack(spacing: 0) {
                    ScrollViewReader { proxy in
                        ScrollView {
                            VStack(alignment: .leading, spacing: 12) {
                                if messages.isEmpty {
                                    Text("Chatting with \(model) on \(target). Local inference — no history is kept server-side.")
                                        .font(Theme.mono(12)).foregroundStyle(Theme.inkFaint).padding(.vertical, 8)
                                }
                                ForEach(messages) { bubble($0) }
                                Color.clear.frame(height: 1).id("bottom")
                            }
                            .padding(16)
                        }
                        .onChange(of: messages.count) { _, _ in withAnimation { proxy.scrollTo("bottom", anchor: .bottom) } }
                    }
                    inputBar
                }
            }
            .navigationTitle(model)
            .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Close") { dismiss() } } }
        }
        .preferredColorScheme(.dark)
    }

    private func bubble(_ m: ChatMsg) -> some View {
        HStack {
            if m.role == "user" { Spacer(minLength: 36) }
            Text(m.text).font(Theme.mono(13))
                .foregroundStyle(m.error ? Theme.red : (m.role == "user" ? Theme.amberHi : Theme.ink))
                .italic(m.thinking).textSelection(.enabled)
                .padding(.horizontal, 13).padding(.vertical, 10)
                .background(m.role == "user" ? Theme.amber.opacity(0.10) : Theme.panel2)
                .clipShape(m.role == "user" ? AnyShape(ChamferBL(cut: 9)) : AnyShape(Chamfer(cut: 9)))
                .overlay((m.role == "user" ? AnyShape(ChamferBL(cut: 9)) : AnyShape(Chamfer(cut: 9)))
                    .stroke(m.error ? Theme.red.opacity(0.5) : Theme.line, lineWidth: 1))
            if m.role == "bot" { Spacer(minLength: 36) }
        }
    }

    private var inputBar: some View {
        HStack(spacing: 10) {
            TextField("ask \(model)…", text: $input, axis: .vertical)
                .font(Theme.mono(14)).foregroundStyle(Theme.ink).lineLimit(1...4)
                .autocorrectionDisabled()
                #if os(iOS)
                .textInputAutocapitalization(.sentences)
                #endif
                .padding(12).background(Theme.panel).overlay(Rectangle().stroke(Theme.edgeHard, lineWidth: 1))
            Button { send() } label: {
                Image(systemName: busy ? "hourglass" : "paperplane.fill")
                    .foregroundStyle(busy ? Theme.inkFaint : Theme.amber)
                    .frame(width: 50, height: 48).overlay(Chamfer(cut: 8).stroke(Theme.edgeHard, lineWidth: 1))
            }.buttonStyle(.plain).disabled(busy)
        }
        .padding(.horizontal, 16).padding(.vertical, 10).background(Theme.bgRaise.opacity(0.5))
    }

    private func send() {
        let text = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !text.isEmpty, !busy else { return }
        input = ""; busy = true
        messages.append(ChatMsg(role: "user", text: text))
        let history = messages.map { ["role": $0.role == "user" ? "user" : "assistant", "content": $0.text] }
        let bot = ChatMsg(role: "bot", text: "…", thinking: true)
        messages.append(bot)
        let id = bot.id

        Task {
            var acc = ""
            do {
                for try await frame in store.api.sseStream("/api/ollama/chat",
                        body: ["target": target, "model": model, "messages": history]) {
                    switch frame.event {
                    case "chunk": if let c = chunkContent(frame.data) { acc += c; update(id, acc) }
                    case "error": update(id, errText(frame.data), error: true)
                    default: break
                    }
                }
                if acc.isEmpty { update(id, "(no reply)") }
            } catch { update(id, error.localizedDescription, error: true) }
            busy = false
        }
    }

    private func update(_ id: UUID, _ text: String, error: Bool = false) {
        guard let i = messages.firstIndex(where: { $0.id == id }) else { return }
        messages[i].text = text; messages[i].thinking = false; messages[i].error = error
    }

    private func chunkContent(_ raw: String) -> String? {
        struct C: Decodable { struct M: Decodable { var content: String? }; var message: M? }
        return (try? JSONDecoder().decode(C.self, from: Data(raw.utf8)))?.message?.content
    }
    private func errText(_ raw: String) -> String {
        struct E: Decodable { var error: String? }
        return (try? JSONDecoder().decode(E.self, from: Data(raw.utf8)))?.error ?? "error"
    }
}
