import SwiftUI

struct LogsResponse: Codable { var logs: String? }
struct K8sEventsResponse: Codable { var events: [K8sEvent]? }
struct K8sEvent: Codable, Identifiable {
    var type: String?
    var reason: String?
    var object: String?
    var message: String?
    var count: Int?
    var last: String?
    var id: String { (object ?? "") + "|" + (reason ?? "") + "|" + (last ?? "") }
}

extension ArgusStore {
    private func enc(_ s: String) -> String { s.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? s }
    func dockerLogs(target: String, name: String, tail: Int = 200) async throws -> String {
        try await api.get("/api/docker/\(enc(target))/\(enc(name))/logs?tail=\(tail)", as: LogsResponse.self).logs ?? ""
    }
    func k8sLogs(ns: String, pod: String, tail: Int = 200) async throws -> String {
        try await api.get("/api/k8s/logs?ns=\(enc(ns))&pod=\(enc(pod))&tail=\(tail)", as: LogsResponse.self).logs ?? ""
    }
    func k8sEvents() async throws -> K8sEventsResponse {
        try await api.get("/api/k8s/events", as: K8sEventsResponse.self)
    }
}

// reusable terminal-style log viewer
struct LogSheet: View {
    var title: String
    var subtitle: String = ""
    var fetch: @MainActor () async throws -> String
    @Environment(\.dismiss) private var dismiss
    @State private var text = "loading…"

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                ScrollView {
                    Text(text.isEmpty ? "(empty)" : text)
                        .font(Theme.mono(10)).foregroundStyle(Theme.hex(0xb7c2cc))
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .textSelection(.enabled)
                        .padding(12).background(Theme.hex(0x07090c))
                        .overlay(Rectangle().stroke(Theme.line, lineWidth: 1))
                        .padding(12)
                }
            }
            .navigationTitle(title)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) { Button("Close") { dismiss() } }
                ToolbarItem(placement: .primaryAction) { Button { Task { await load() } } label: { Image(systemName: "arrow.clockwise") } }
            }
        }
        .preferredColorScheme(.dark)
        .task { await load() }
    }
    private func load() async {
        do { text = try await fetch() } catch { text = "logs failed: \(error.localizedDescription)" }
    }
}

// self-loading k8s events panel (used at top of K8sView)
struct K8sEventsPanel: View {
    @EnvironmentObject var store: ArgusStore
    @State private var events: [K8sEvent] = []

    var body: some View {
        Group {
            if !events.isEmpty {
                PanelSection(title: "EVENTS", systemImage: "exclamationmark.bubble", trailing: "\(events.count)") {
                    ForEach(events) { e in
                        VStack(alignment: .leading, spacing: 2) {
                            HStack {
                                Text(e.reason ?? "").font(Theme.display(10)).tracking(1)
                                    .foregroundStyle(e.type == "Warning" ? Theme.amber : Theme.inkDim)
                                Spacer()
                                if let c = e.count, c > 1 { Text("×\(c)").font(Theme.mono(9)).foregroundStyle(Theme.inkFaint) }
                            }
                            Text(e.object ?? "").font(Theme.mono(10)).foregroundStyle(Theme.ink).lineLimit(1)
                            Text(e.message ?? "").font(Theme.mono(9.5)).foregroundStyle(Theme.inkFaint).lineLimit(3)
                        }
                        .padding(.vertical, 5)
                        .overlay(alignment: .bottom) { Rectangle().fill(Theme.line).frame(height: 1) }
                    }
                }
            }
        }
        .task { while !Task.isCancelled { events = (try? await store.k8sEvents().events) ?? events; try? await Task.sleep(nanoseconds: 15_000_000_000) } }
    }
}
