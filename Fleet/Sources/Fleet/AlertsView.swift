import SwiftUI

struct AlertsView: View {
    @EnvironmentObject var fleet: FleetStore
    @State private var showCleared = false

    var visible: [AlertItem] {
        showCleared ? fleet.alerts : fleet.alerts.filter(\.isActive)
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                ScrollView {
                    LazyVStack(spacing: 10) {
                        Toggle(isOn: $showCleared) { Text("Show cleared") }
                            .padding(.horizontal, 16)
                            .padding(.top, 8)
                        if visible.isEmpty {
                            VStack(spacing: 8) {
                                Image(systemName: "checkmark.circle.fill")
                                    .font(.system(size: 44))
                                    .foregroundStyle(Theme.ok)
                                Text("All clear")
                                    .font(.headline)
                                Text(fleet.lastUpdate.map { "Last refresh \(relativeFormatter.localizedString(for: $0, relativeTo: Date()))" } ?? "")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            .padding(40)
                        } else {
                            ForEach(visible) { alert in
                                AlertRow(alert: alert)
                            }
                            .padding(.horizontal, 16)
                        }
                    }
                }
                .refreshable { await fleet.refresh() }
            }
            .navigationTitle("Alerts")
        }
    }
}

struct AlertRow: View {
    let alert: AlertItem
    var color: Color { Theme.severity(alert.severity) }

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text((alert.rule_name ?? "alert").uppercased())
                    .font(.caption.weight(.bold))
                    .foregroundStyle(color)
                Spacer()
                if !alert.isActive {
                    Text("CLEARED").font(.caption2.bold()).foregroundStyle(.secondary)
                }
            }
            Text(alert.message).font(.subheadline)
            HStack(spacing: 10) {
                Text(alert.hostLabel).font(.caption).foregroundStyle(.secondary)
                if let v = alert.last_value {
                    Text(String(format: "value %.2f", v)).font(.caption2).foregroundStyle(.secondary)
                }
                Spacer()
                Text(relativeFormatter.localizedString(
                    for: Date(timeIntervalSince1970: TimeInterval(alert.fired_at)),
                    relativeTo: Date()
                ))
                .font(.caption)
                .foregroundStyle(.secondary)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(12)
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Theme.panel)
                .overlay(
                    RoundedRectangle(cornerRadius: 12).strokeBorder(color.opacity(0.5), lineWidth: 1)
                )
        )
    }
}
