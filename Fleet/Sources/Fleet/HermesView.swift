import SwiftUI

// MARK: - SSE-over-POST (Hermes / Ollama streaming)

struct SSEFrame: Sendable { var event: String; var data: String }

extension ArgusAPI {
    func sseStream(_ path: String, body: [String: any Sendable]) -> AsyncThrowingStream<SSEFrame, Error> {
        let base = self.base
        return AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    guard let u = URL(string: base + path) else { throw APIError.badURL }
                    var req = URLRequest(url: u)
                    req.httpMethod = "POST"
                    req.timeoutInterval = 300
                    req.setValue("application/json", forHTTPHeaderField: "Content-Type")
                    req.setValue("argus-ui", forHTTPHeaderField: "X-Argus-Confirm")
                    req.httpBody = try JSONSerialization.data(withJSONObject: body)
                    let (bytes, resp) = try await URLSession.shared.bytes(for: req)
                    if let h = resp as? HTTPURLResponse, !(200..<300).contains(h.statusCode) {
                        throw APIError.http(h.statusCode)
                    }
                    var ev = "message"; var buf = ""
                    for try await line in bytes.lines {
                        if line.isEmpty {
                            if !buf.isEmpty { continuation.yield(SSEFrame(event: ev, data: buf)) }
                            ev = "message"; buf = ""
                        } else if line.hasPrefix("event:") {
                            ev = String(line.dropFirst(6)).trimmingCharacters(in: .whitespaces)
                        } else if line.hasPrefix("data:") {
                            buf += String(line.dropFirst(5)).trimmingCharacters(in: .whitespaces)
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}

// MARK: - Chat

struct ChatMsg: Identifiable {
    let id = UUID()
    var role: String          // "user" | "bot"
    var text: String
    var thinking = false
    var error = false
}

struct HermesView: View {
    @EnvironmentObject var store: ArgusStore
    @State private var messages: [ChatMsg] = []
    @State private var input = ""
    @State private var busy = false
    @State private var activity = ""
    @State private var status: HermesStatus?

    var body: some View {
        VStack(spacing: 0) {
            statusBar
            ScrollViewReader { proxy in
                ScrollView {
                    VStack(alignment: .leading, spacing: 12) {
                        if messages.isEmpty {
                            Text("☤ Hermes is your agent on ubuntu4070ti. Ask it anything — it can run tools on its host.")
                                .font(Theme.mono(12)).foregroundStyle(Theme.inkFaint).padding(.vertical, 8)
                        }
                        ForEach(messages) { bubble($0) }
                        if !activity.isEmpty {
                            Text(activity.trimmingCharacters(in: .whitespacesAndNewlines))
                                .font(Theme.mono(10)).foregroundStyle(Theme.inkFaint)
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .padding(8).background(Theme.hex(0x07090c))
                                .overlay(Rectangle().stroke(Theme.line, lineWidth: 1))
                        }
                        Color.clear.frame(height: 1).id("bottom")
                    }
                    .padding(16)
                }
                .onChange(of: messages.count) { _, _ in withAnimation { proxy.scrollTo("bottom", anchor: .bottom) } }
            }
            quickPrompts
            inputBar
        }
        .task { status = (try? await store.api.get("/api/hermes", as: HermesResponse.self))?.status }
    }

    private var statusBar: some View {
        HStack(spacing: 10) {
            let up = status?.gateway_active ?? false
            HStack(spacing: 6) {
                Circle().fill(up ? Theme.green : Theme.red).frame(width: 8, height: 8)
                Text("GATEWAY \(up ? "ACTIVE" : "DOWN")").font(Theme.display(9.5)).tracking(1.4).foregroundStyle(up ? Theme.green : Theme.red)
            }
            if let m = status?.mode { Text("· \(m)").font(Theme.mono(10)).foregroundStyle(Theme.inkFaint) }
            Spacer()
            Button { messages = []; activity = "" } label: {
                Text("CLEAR").font(Theme.display(9.5)).tracking(1.2).foregroundStyle(Theme.inkDim)
            }.buttonStyle(.plain)
        }
        .padding(.horizontal, 16).padding(.vertical, 10)
        .background(Theme.bgRaise.opacity(0.5))
    }

    private func bubble(_ m: ChatMsg) -> some View {
        HStack {
            if m.role == "user" { Spacer(minLength: 40) }
            VStack(alignment: .leading, spacing: 4) {
                if m.role == "bot" {
                    Text("HERMES ☤").font(Theme.display(8.5)).tracking(2).foregroundStyle(Theme.inkFaint)
                }
                Text(m.text)
                    .font(Theme.mono(13))
                    .foregroundStyle(m.error ? Theme.red : (m.role == "user" ? Theme.amberHi : Theme.ink))
                    .italic(m.thinking)
                    .textSelection(.enabled)
            }
            .padding(.horizontal, 14).padding(.vertical, 11)
            .background(m.role == "user" ? Theme.amber.opacity(0.10) : Theme.panel2)
            .clipShape(m.role == "user" ? AnyShape(ChamferBL(cut: 9)) : AnyShape(Chamfer(cut: 9)))
            .overlay((m.role == "user" ? AnyShape(ChamferBL(cut: 9)) : AnyShape(Chamfer(cut: 9)))
                .stroke(m.error ? Theme.red.opacity(0.5) : (m.role == "user" ? Theme.edgeHard : Theme.line), lineWidth: 1))
            if m.role == "bot" { Spacer(minLength: 40) }
        }
    }

    private var quickPrompts: some View {
        let prompts = store.uiConfig?.hermes_prompts ?? []
        return Group {
            if !prompts.isEmpty && messages.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(prompts, id: \.self) { p in
                            Button { input = p; send() } label: {
                                Text(p.count > 40 ? String(p.prefix(38)) + "…" : p)
                                    .font(Theme.mono(10)).foregroundStyle(Theme.inkDim)
                                    .padding(.horizontal, 10).padding(.vertical, 7)
                                    .overlay(Chamfer(cut: 6).stroke(Theme.line, lineWidth: 1))
                            }.buttonStyle(.plain)
                        }
                    }.padding(.horizontal, 16).padding(.bottom, 6)
                }
            }
        }
    }

    private var inputBar: some View {
        HStack(spacing: 10) {
            TextField("message Hermes…", text: $input, axis: .vertical)
                .font(Theme.mono(14)).foregroundStyle(Theme.ink)
                .lineLimit(1...4)
                .autocorrectionDisabled()
                #if os(iOS)
                .textInputAutocapitalization(.sentences)
                #endif
                .padding(12)
                .background(Theme.panel)
                .overlay(Rectangle().stroke(Theme.edgeHard, lineWidth: 1))
            Button { send() } label: {
                Image(systemName: busy ? "hourglass" : "paperplane.fill")
                    .foregroundStyle(busy ? Theme.inkFaint : Theme.amber)
                    .frame(width: 50, height: 48)
                    .overlay(Chamfer(cut: 8).stroke(Theme.edgeHard, lineWidth: 1))
            }.buttonStyle(.plain).disabled(busy)
        }
        .padding(.horizontal, 16).padding(.vertical, 10)
        .background(Theme.bgRaise.opacity(0.5))
    }

    private func send() {
        let text = input.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !text.isEmpty, !busy else { return }
        input = ""; busy = true; activity = ""
        messages.append(ChatMsg(role: "user", text: text))
        let bot = ChatMsg(role: "bot", text: "thinking…", thinking: true)
        messages.append(bot)
        let botID = bot.id

        Task {
            var finalText = ""
            do {
                for try await frame in store.api.sseStream("/api/hermes/chat", body: ["message": text]) {
                    let inner = decodeData(frame.data)
                    switch frame.event {
                    case "log":   if !inner.isEmpty { activity += inner + "\n" }
                    case "text":  finalText = inner; updateBot(botID, text: inner)
                    case "error": updateBot(botID, text: inner.isEmpty ? "error" : inner, error: true)
                    default: break
                    }
                }
                if finalText.isEmpty { updateBot(botID, text: "(no reply — check gateway)") }
            } catch {
                updateBot(botID, text: error.localizedDescription, error: true)
            }
            busy = false
        }
    }

    private func updateBot(_ id: UUID, text: String, error: Bool = false) {
        guard let i = messages.firstIndex(where: { $0.id == id }) else { return }
        messages[i].text = text
        messages[i].thinking = false
        messages[i].error = error
    }

    private func decodeData(_ raw: String) -> String {
        struct E: Decodable { var data: String? }
        return (try? JSONDecoder().decode(E.self, from: Data(raw.utf8)))?.data ?? ""
    }
}
