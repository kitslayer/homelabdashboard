import SwiftUI

// ════════════════════════════════════════════════════════════════════
//  Argus design tokens — shared 1:1 with the kiosk (argus/static/css/argus.css)
//  "phosphor-amber mission console"
// ════════════════════════════════════════════════════════════════════

enum Theme {
    // palette
    static let bg       = hex(0x0b0d11)
    static let bgRaise  = hex(0x10131a)
    static let panel    = hex(0x12151d)
    static let panel2   = hex(0x171b25)
    static let ink      = hex(0xe9e4d6)
    static let inkDim   = hex(0x9aa3ad)
    static let inkFaint = hex(0x5c6672)
    static let amber    = hex(0xffb000)
    static let amberHi  = hex(0xffd166)
    static let amberDn  = hex(0x8a6200)
    static let green    = hex(0x58d68b)
    static let greenDim = hex(0x2c7a4d)
    static let red      = hex(0xf4564e)
    static let redDim   = hex(0x8e2b27)
    static let blue     = hex(0x6ea8fe)

    static let edge     = Color(red: 1, green: 0.690, blue: 0, opacity: 0.09)
    static let edgeHard = Color(red: 1, green: 0.690, blue: 0, opacity: 0.22)
    static let line     = Color(red: 0.553, green: 0.620, blue: 0.706, opacity: 0.10)

    // alias used widely
    static let accent = amber
    static let chamfer: CGFloat = 12

    static func hex(_ v: UInt32) -> Color {
        Color(red: Double((v >> 16) & 0xff) / 255,
              green: Double((v >> 8) & 0xff) / 255,
              blue: Double(v & 0xff) / 255)
    }

    // ── typography ───────────────────────────────────────────────────
    // "display" = wide, letter-spaced, uppercase (Chakra Petch feel)
    // "mono"    = tabular numbers (Martian Mono feel)
    // Real TTFs get bundled later; system fallbacks read on-brand already.
    static func display(_ size: CGFloat, _ weight: Font.Weight = .bold) -> Font {
        let face = weight == .bold ? "ChakraPetch-Bold"
                 : (weight == .medium ? "ChakraPetch-Medium" : "ChakraPetch-Regular")
        return .custom(face, size: size)   // falls back to system if unregistered
    }
    static func mono(_ size: CGFloat, _ weight: Font.Weight = .regular) -> Font {
        .system(size: size, weight: weight, design: .monospaced)
    }

    // ── status helpers (mirror ui.js / fmt.js) ───────────────────────
    static func severity(_ s: String) -> Color {
        switch s {
        case "critical": return red
        case "warning":  return amber
        case "info":     return blue
        default:         return inkDim
        }
    }

    /// gauge / percentage color with Argus thresholds
    static func pctColor(_ p: Double?, warn: Double = 75, crit: Double = 90) -> Color {
        guard let p else { return inkFaint }
        if p >= crit { return red }
        if p >= warn { return amber }
        return green
    }

    /// statusChip mapping from ui.js
    static func statusColor(_ s: String) -> Color {
        switch s.lowercased() {
        case "running", "online", "up", "passed", "ok": return green
        case "stopped", "exited", "idle":               return inkFaint
        case "offline", "unknown", "down", "bad":       return red
        case "paused", "warn", "warning":               return amber
        default:                                         return amber
        }
    }
}

// ── the chamfer signature: top-right cut corner ──────────────────────
// matches CSS clip-path: polygon(0 0, calc(100%-c) 0, 100% c, 100% 100%, 0 100%)
struct Chamfer: Shape {
    var cut: CGFloat = Theme.chamfer
    func path(in r: CGRect) -> Path {
        let c = min(cut, min(r.width, r.height))
        var p = Path()
        p.move(to: CGPoint(x: r.minX, y: r.minY))
        p.addLine(to: CGPoint(x: r.maxX - c, y: r.minY))
        p.addLine(to: CGPoint(x: r.maxX, y: r.minY + c))
        p.addLine(to: CGPoint(x: r.maxX, y: r.maxY))
        p.addLine(to: CGPoint(x: r.minX, y: r.maxY))
        p.closeSubpath()
        return p
    }
}

// bottom-left cut (used for "user" chat bubbles, mirror of the panel)
struct ChamferBL: Shape {
    var cut: CGFloat = 9
    func path(in r: CGRect) -> Path {
        let c = min(cut, min(r.width, r.height))
        var p = Path()
        p.move(to: CGPoint(x: r.minX, y: r.minY))
        p.addLine(to: CGPoint(x: r.maxX, y: r.minY))
        p.addLine(to: CGPoint(x: r.maxX, y: r.maxY))
        p.addLine(to: CGPoint(x: r.minX + c, y: r.maxY))
        p.addLine(to: CGPoint(x: r.minX, y: r.maxY - c))
        p.closeSubpath()
        return p
    }
}

struct PanelStyle: ViewModifier {
    var pad: CGFloat = 14
    var fill: Color = Theme.panel
    func body(content: Content) -> some View {
        content
            .padding(pad)
            .background(Chamfer().fill(fill))
            .overlay(Chamfer().stroke(Theme.line, lineWidth: 1))
    }
}

extension View {
    func argusPanel(_ pad: CGFloat = 14, fill: Color = Theme.panel) -> some View {
        modifier(PanelStyle(pad: pad, fill: fill))
    }
    /// wide, letter-spaced, uppercase label (Chakra Petch feel)
    func displayLabel(_ size: CGFloat = 13, tracking: CGFloat = 3, color: Color = Theme.inkDim) -> some View {
        self.font(Theme.display(size)).tracking(tracking).textCase(.uppercase).foregroundStyle(color)
    }
}
