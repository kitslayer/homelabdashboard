import SwiftUI

// Reusable: a scroll view that loads + auto-polls a fetch closure.
struct PollingScroll<T, Content: View>: View {
    let fetch: @MainActor () async throws -> T
    var interval: Double = 10
    @ViewBuilder var content: (T) -> Content

    @State private var data: T?
    @State private var err: String?

    var body: some View {
        ScrollView {
            VStack(spacing: 12) {
                if let d = data {
                    content(d)
                } else if let e = err {
                    ErrPanel(message: e)
                } else {
                    ProgressView().tint(Theme.amber).frame(maxWidth: .infinity).padding(.vertical, 50)
                }
            }
            .padding(.horizontal, 16).padding(.vertical, 14)
        }
        .refreshable { await once() }
        .task { while !Task.isCancelled { await once(); try? await Task.sleep(nanoseconds: UInt64(interval * 1_000_000_000)) } }
    }

    private func once() async {
        do { data = try await fetch(); err = nil } catch { err = error.localizedDescription }
    }
}

struct ErrPanel: View {
    var message: String
    var body: some View {
        HStack(alignment: .top, spacing: 8) {
            Image(systemName: "exclamationmark.triangle").foregroundStyle(Theme.red)
            Text(message).font(Theme.mono(11)).foregroundStyle(Theme.red)
            Spacer(minLength: 0)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .argusPanel()
    }
}

// A titled panel section.
struct PanelSection<Content: View>: View {
    var title: String
    var systemImage: String? = nil
    var trailing: String? = nil
    @ViewBuilder var content: () -> Content

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            if let trailing { PanelTitle(text: title, systemImage: systemImage) { Text(trailing) } }
            else { PanelTitle(text: title, systemImage: systemImage) }
            content()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .argusPanel()
    }
}

// A status row: dot + name (+ detail) + trailing value.
struct DotRow: View {
    var up: Bool?
    var name: String
    var detail: String? = nil
    var trailing: String? = nil
    var trailingColor: Color = Theme.inkDim

    var body: some View {
        HStack(spacing: 10) {
            if let up {
                Circle().fill(up ? Theme.green : Theme.red).frame(width: 7, height: 7)
            }
            VStack(alignment: .leading, spacing: 1) {
                Text(name).font(Theme.mono(12.5)).foregroundStyle(Theme.ink).lineLimit(1)
                if let detail, !detail.isEmpty {
                    Text(detail).font(Theme.mono(10)).foregroundStyle(Theme.inkFaint).lineLimit(1)
                }
            }
            Spacer(minLength: 4)
            if let trailing { Text(trailing).font(Theme.mono(11)).foregroundStyle(trailingColor) }
        }
        .padding(.vertical, 7)
        .overlay(alignment: .bottom) { Rectangle().fill(Theme.line).frame(height: 1) }
    }
}

// A power action for the reusable action sheet.
struct PowerAction: Identifiable {
    let id = UUID()
    var label: String
    var hint: String = "HOLD"
    var destructive: Bool
    var ms: Double = 900
    var run: @MainActor () async throws -> Void
}

// Reusable bottom sheet for entity actions (VM / container / pod / host).
struct ActionSheet2: View {
    var title: String
    var subtitle: String
    var status: String
    var actions: [PowerAction]
    var logs: (@MainActor () async throws -> String)? = nil
    var statsLine: (@MainActor () async throws -> String)? = nil

    @Environment(\.dismiss) private var dismiss
    @State private var msg: String?
    @State private var msgErr = false
    @State private var showLogs = false
    @State private var statsText: String?

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                ScrollView {
                    VStack(alignment: .leading, spacing: 14) {
                        PanelSection(title: title) {
                            HStack {
                                Text(subtitle).font(Theme.mono(12)).foregroundStyle(Theme.inkDim)
                                Spacer()
                                StatusChip(status: status)
                            }
                            if let statsText {
                                Text(statsText).font(Theme.mono(11)).foregroundStyle(Theme.amber)
                                    .frame(maxWidth: .infinity, alignment: .leading).padding(.top, 2)
                            }
                        }
                        VStack(spacing: 10) {
                            ForEach(actions) { a in
                                if a.destructive {
                                    HoldButton(label: a.label, hint: a.hint, ms: a.ms, role: .danger) { await run(a) }
                                } else {
                                    Button { Task { await run(a) } } label: {
                                        Text(a.label).font(Theme.display(12.5)).tracking(1.4)
                                            .foregroundStyle(Theme.amber)
                                            .frame(maxWidth: .infinity, minHeight: 46)
                                            .background(Theme.amber.opacity(0.05))
                                            .clipShape(Chamfer(cut: 9))
                                            .overlay(Chamfer(cut: 9).stroke(Theme.edgeHard, lineWidth: 1))
                                    }.buttonStyle(.plain)
                                }
                            }
                        }
                        if logs != nil {
                            Button { showLogs = true } label: {
                                Label("VIEW LOGS", systemImage: "doc.plaintext")
                                    .font(Theme.display(12)).tracking(1.2).foregroundStyle(Theme.inkDim)
                                    .frame(maxWidth: .infinity, minHeight: 44)
                                    .overlay(Chamfer(cut: 9).stroke(Theme.line, lineWidth: 1))
                            }.buttonStyle(.plain)
                        }
                        if let msg {
                            Text(msg).font(Theme.mono(12)).foregroundStyle(msgErr ? Theme.red : Theme.green)
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                        Spacer()
                    }
                    .padding(16)
                }
            }
            .navigationTitle(title)
            .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Close") { dismiss() } } }
        }
        .preferredColorScheme(.dark)
        .sheet(isPresented: $showLogs) { if let logs { LogSheet(title: title, subtitle: subtitle, fetch: logs) } }
        .task {
            guard let statsLine else { return }
            while !Task.isCancelled { statsText = try? await statsLine(); try? await Task.sleep(nanoseconds: 3_000_000_000) }
        }
    }

    private func run(_ a: PowerAction) async {
        do { try await a.run(); msg = "\(a.label.lowercased()) sent ✓"; msgErr = false; Haptics.success() }
        catch { msg = error.localizedDescription; msgErr = true; Haptics.failure() }
    }
}
