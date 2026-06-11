import SwiftUI

enum Theme {
    static let accent = Color(red: 0.31, green: 0.56, blue: 0.97)
    static let ok = Color(red: 0.13, green: 0.77, blue: 0.37)
    static let warn = Color(red: 0.96, green: 0.62, blue: 0.04)
    static let crit = Color(red: 0.94, green: 0.27, blue: 0.27)
    static let info = Color(red: 0.02, green: 0.71, blue: 0.83)
    static let bg = Color(red: 0.04, green: 0.05, blue: 0.07)
    static let panel = Color(red: 0.07, green: 0.09, blue: 0.11)
    static let line = Color.white.opacity(0.08)

    static func severity(_ s: String) -> Color {
        switch s {
        case "critical": return crit
        case "warning": return warn
        case "info": return info
        default: return .secondary
        }
    }

    static func pctColor(_ p: Double?) -> Color {
        guard let p else { return .secondary }
        if p >= 90 { return crit }
        if p >= 75 { return warn }
        return accent
    }
}

struct PanelStyle: ViewModifier {
    func body(content: Content) -> some View {
        content
            .padding(14)
            .background(
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .fill(Theme.panel)
                    .overlay(
                        RoundedRectangle(cornerRadius: 14, style: .continuous)
                            .strokeBorder(Theme.line, lineWidth: 1)
                    )
            )
    }
}

extension View {
    func fleetPanel() -> some View { self.modifier(PanelStyle()) }
}
