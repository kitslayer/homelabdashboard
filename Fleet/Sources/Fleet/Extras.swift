import SwiftUI

// MARK: - Speedtest  (/api/tools/speedtest, /api/tools/speedtest/last)

struct SpeedtestLast: Codable { var configured: Bool?; var running: Bool?; var history: [SpeedRun]? }
struct SpeedRun: Codable {
    var ts: Double?; var ping_ms: Double?; var jitter_ms: Double?
    var down_mbps: Double?; var up_mbps: Double?; var server: String?; var err: String?
}

// MARK: - Recent actions / audit  (/api/actions/recent)

struct ActionsResponse: Codable { var actions: [ActionItem]? }
struct ActionItem: Codable, Identifiable {
    var ts: Double?; var action: String?; var target: String?; var ok: Bool?; var detail: String?
    var id: String { "\(ts ?? 0)|\(action ?? "")|\(target ?? "")" }
}

extension ArgusStore {
    func speedtestLast() async throws -> SpeedtestLast { try await api.get("/api/tools/speedtest/last", as: SpeedtestLast.self) }
    func runSpeedtest() async throws { try await api.post("/api/tools/speedtest") }
    func actionsRecent() async throws -> ActionsResponse { try await api.get("/api/actions/recent", as: ActionsResponse.self) }
}

// self-loading speedtest panel (top of NetworkView)
struct SpeedtestPanel: View {
    @EnvironmentObject var store: ArgusStore
    @State private var data: SpeedtestLast?
    @State private var msg: String?

    var body: some View {
        let last = data?.history?.last
        PanelSection(title: "SPEEDTEST", systemImage: "speedometer",
                     trailing: data?.running == true ? "RUNNING…" : nil) {
            if let last, (last.err ?? "").isEmpty {
                HStack(spacing: 16) {
                    metric("DOWN", last.down_mbps, "Mbps", Theme.green)
                    metric("UP", last.up_mbps, "Mbps", Theme.amber)
                    metric("PING", last.ping_ms, "ms", Theme.blue)
                }
                if let s = last.server { Text(s).font(Theme.mono(9.5)).foregroundStyle(Theme.inkFaint).lineLimit(1) }
                Text(Fmt.ago(last.ts.map { Int($0) })).font(Theme.mono(9)).foregroundStyle(Theme.inkFaint)
            } else if let e = last?.err, !e.isEmpty {
                Text(e).font(Theme.mono(10)).foregroundStyle(Theme.red).lineLimit(2)
            } else {
                Text("no results yet").font(Theme.mono(11)).foregroundStyle(Theme.inkFaint)
            }
            if data?.configured == true {
                HoldButton(label: "RUN SPEEDTEST", ms: 700, role: .normal) {
                    do { try await store.runSpeedtest(); msg = "done" } catch { msg = error.localizedDescription }
                    await load()
                }.padding(.top, 4)
            }
            if let msg { Text(msg).font(Theme.mono(10)).foregroundStyle(Theme.inkFaint) }
        }
        .task { while !Task.isCancelled { await load(); try? await Task.sleep(nanoseconds: 12_000_000_000) } }
    }
    private func load() async { data = (try? await store.speedtestLast()) ?? data }
    private func metric(_ l: String, _ v: Double?, _ u: String, _ c: Color) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(v.map { String(format: "%.0f", $0) } ?? "—").font(Theme.mono(20, .bold)).foregroundStyle(c)
            Text("\(l) \(u)").font(Theme.display(8.5)).tracking(1).foregroundStyle(Theme.inkFaint)
        }
    }
}

// self-loading recent-activity panel (Settings)
struct ActivityPanel: View {
    @EnvironmentObject var store: ArgusStore
    @State private var items: [ActionItem] = []
    var body: some View {
        Group {
            if !items.isEmpty {
                PanelSection(title: "RECENT ACTIVITY", systemImage: "list.bullet.rectangle", trailing: "\(items.count)") {
                    ForEach(items.prefix(30)) { a in
                        VStack(alignment: .leading, spacing: 1) {
                            HStack(spacing: 6) {
                                Circle().fill((a.ok ?? true) ? Theme.green : Theme.red).frame(width: 6, height: 6)
                                Text(a.action ?? "").font(Theme.mono(11)).foregroundStyle(Theme.ink)
                                Text(a.target ?? "").font(Theme.mono(10)).foregroundStyle(Theme.inkFaint).lineLimit(1)
                                Spacer()
                                Text(Fmt.ago(a.ts.map { Int($0) })).font(Theme.mono(9)).foregroundStyle(Theme.inkFaint)
                            }
                            if let d = a.detail, !d.isEmpty {
                                Text(d).font(Theme.mono(9.5)).foregroundStyle(Theme.inkFaint).lineLimit(2)
                            }
                        }
                        .padding(.vertical, 4)
                        .overlay(alignment: .bottom) { Rectangle().fill(Theme.line).frame(height: 1) }
                    }
                }
            }
        }
        .task { items = (try? await store.actionsRecent().actions) ?? [] }
    }
}
