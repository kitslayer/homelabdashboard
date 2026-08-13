import SwiftUI

// ════════════════════════════════════════════════════════════════════
//  Argus UI primitives (ports of ui.js / hold.js)
// ════════════════════════════════════════════════════════════════════

// MARK: - Gauge (industrial tick-mark fuel bar)

struct Gauge: View {
    var label: String
    var pct: Double?
    var warn: Double = 75
    var crit: Double = 90

    var body: some View {
        HStack(spacing: 8) {
            if !label.isEmpty {
                Text(label).font(Theme.mono(10.5)).foregroundStyle(Theme.inkFaint)
                    .frame(width: 34, alignment: .leading)
            }
            GeometryReader { geo in
                ZStack(alignment: .leading) {
                    Rectangle().fill(Theme.hex(0x0a0c10))
                    // ticks every 10%
                    HStack(spacing: 0) {
                        ForEach(0..<10) { _ in
                            Rectangle().fill(Color(red: 0.55, green: 0.62, blue: 0.71, opacity: 0.18))
                                .frame(width: 1)
                            Spacer(minLength: 0)
                        }
                    }
                    Rectangle()
                        .fill(Theme.pctColor(pct, warn: warn, crit: crit))
                        .frame(width: max(0, geo.size.width * CGFloat((pct ?? 0).clamped(0, 100) / 100)))
                        .animation(.easeOut(duration: 0.5), value: pct)
                }
                .overlay(Rectangle().stroke(Theme.line, lineWidth: 1))
            }
            .frame(height: 10)
            Text(Fmt.pct(pct)).font(Theme.mono(11)).foregroundStyle(Theme.ink)
                .frame(width: 42, alignment: .trailing)
        }
    }
}

// MARK: - Status chip

struct StatusChip: View {
    var text: String
    var color: Color
    init(_ text: String, color: Color) { self.text = text; self.color = color }
    init(status: String) { self.text = status; self.color = Theme.statusColor(status) }

    var body: some View {
        Text(text.uppercased())
            .font(Theme.display(10)).tracking(1.2)
            .foregroundStyle(color)
            .padding(.horizontal, 9).padding(.vertical, 3)
            .overlay(Rectangle().stroke(color, lineWidth: 1))
    }
}

// MARK: - Count card

struct CountCard: View {
    var big: String
    var label: String
    var color: Color = Theme.green

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(big).font(Theme.mono(28, .bold)).foregroundStyle(color)
                .lineLimit(1).minimumScaleFactor(0.6)
            Text(label).font(Theme.display(9.5)).tracking(2).foregroundStyle(Theme.inkFaint)
                .lineLimit(1).minimumScaleFactor(0.7)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .argusPanel(13)
    }
}

// MARK: - Group label (spaced uppercase + trailing rule)

struct GroupLabel: View {
    var text: String
    var body: some View {
        HStack(spacing: 12) {
            Text(text).font(Theme.display(11)).tracking(3).foregroundStyle(Theme.inkFaint)
            Rectangle().fill(Theme.line).frame(height: 1)
        }
        .padding(.top, 6)
    }
}

// MARK: - Panel header (icon + title + optional trailing)

struct PanelTitle<Trailing: View>: View {
    var text: String
    var systemImage: String?
    @ViewBuilder var trailing: () -> Trailing

    var body: some View {
        HStack(spacing: 10) {
            if let systemImage {
                Image(systemName: systemImage).font(.system(size: 13)).foregroundStyle(Theme.amber)
            }
            Text(text).font(Theme.display(13)).tracking(2.4).foregroundStyle(Theme.inkDim)
            Spacer(minLength: 6)
            trailing().font(Theme.mono(11)).foregroundStyle(Theme.inkFaint)
        }
        .padding(.bottom, 10)
    }
}
extension PanelTitle where Trailing == EmptyView {
    init(text: String, systemImage: String? = nil) {
        self.init(text: text, systemImage: systemImage, trailing: { EmptyView() })
    }
}

// MARK: - Host tile

struct HostTile: View {
    var host: HostView
    var compact: Bool = false

    private var borderColor: Color {
        if host.up { return Theme.green }
        if isExpectedDown { return Theme.inkFaint }
        return Theme.red
    }
    private var isExpectedDown: Bool {
        host.groupKey == "remote" || ["t30", "surface", "qbt"].contains(host.name)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                Text(host.label).font(Theme.display(15.5)).tracking(0.5).foregroundStyle(Theme.ink)
                    .lineLimit(1)
                Spacer(minLength: 4)
                Text(host.ip ?? (host.pve?.vmid.map { "vmid \($0)" } ?? ""))
                    .font(Theme.mono(10.5)).foregroundStyle(Theme.inkFaint)
            }

            if host.up, let f = host.fleet {
                HStack(spacing: 14) {
                    if let c = f.cpu_pct { stat("CPU", Fmt.pct(c)) }
                    if let m = f.mem_pct { stat("MEM", Fmt.pct(m)) }
                    if let t = f.cpu_temp { stat(nil, Fmt.temp(t)) }
                    if let g = f.gpu?.first?.util_pct { stat("GPU", Fmt.pct(g)) }
                }
                if !compact {
                    if let c = f.cpu_pct { Gauge(label: "CPU", pct: c) }
                    if let m = f.mem_pct { Gauge(label: "MEM", pct: m) }
                }
            } else if host.up {
                Text(host.ping_ms.map { String(format: "ping %.1fms", $0) } ?? "reachable")
                    .font(Theme.mono(11)).foregroundStyle(Theme.inkFaint)
            } else {
                Text(isExpectedDown ? "offline — \(host.note?.isEmpty == false ? host.note! : "expected")"
                                    : "DOWN \(Fmt.ago(host.since))")
                    .font(Theme.mono(11))
                    .foregroundStyle(isExpectedDown ? Theme.inkFaint : Theme.red)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(EdgeInsets(top: 12, leading: 14, bottom: 12, trailing: 14))
        .background(Chamfer().fill(Theme.panel))
        .overlay(Chamfer().stroke(Theme.line, lineWidth: 1))
        .overlay(alignment: .leading) { Rectangle().fill(borderColor).frame(width: 3) }
        .opacity(host.up || !isExpectedDown ? 1 : 0.82)
    }

    private func stat(_ label: String?, _ value: String) -> some View {
        HStack(spacing: 3) {
            if let label { Text(label).font(Theme.mono(11)).foregroundStyle(Theme.inkDim) }
            Text(value).font(Theme.mono(11)).foregroundStyle(Theme.ink)
        }
    }
}

// MARK: - Hold-to-confirm button (port of hold.js)

struct HoldButton: View {
    var label: String
    var hint: String = "HOLD"
    var ms: Double = 900
    var role: Role = .danger
    var action: () async -> Void

    enum Role { case danger, normal, ghost
        var tint: Color { self == .danger ? Theme.red : (self == .ghost ? Theme.inkDim : Theme.amber) }
        var border: Color { self == .danger ? Theme.red.opacity(0.4) : (self == .ghost ? Theme.line : Theme.edgeHard) }
        var bg: Color { self == .danger ? Theme.red.opacity(0.06) : (self == .ghost ? .clear : Theme.amber.opacity(0.05)) }
    }

    @State private var holding = false
    @State private var firing = false

    var body: some View {
        ZStack {
            GeometryReader { geo in
                Rectangle().fill(Theme.red.opacity(0.38))
                    .frame(width: holding ? geo.size.width : 0)
            }
            VStack(spacing: 1) {
                Text(label).font(Theme.display(12.5)).tracking(1.4)
                Text(hint).font(Theme.display(8.5)).tracking(1).opacity(0.55)
            }
            .foregroundStyle(role.tint)
        }
        .frame(maxWidth: .infinity, minHeight: 46)
        .background(role.bg)
        .clipShape(Chamfer(cut: 9))
        .overlay(Chamfer(cut: 9).stroke(role.border, lineWidth: 1))
        .opacity(firing ? 0.6 : 1)
        .contentShape(Rectangle())
        .onLongPressGesture(minimumDuration: ms / 1000, maximumDistance: 40) {
            guard !firing else { return }
            Haptics.fire()
            firing = true
            holding = false
            Task { await action(); firing = false }
        } onPressingChanged: { pressing in
            withAnimation(.linear(duration: pressing ? ms / 1000 : 0.18)) { holding = pressing }
        }
    }
}

// MARK: - helpers

extension Comparable {
    func clamped(_ lo: Self, _ hi: Self) -> Self { min(max(self, lo), hi) }
}
