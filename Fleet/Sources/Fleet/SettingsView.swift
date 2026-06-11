import SwiftUI

struct SettingsView: View {
    @EnvironmentObject var session: Session
    @EnvironmentObject var fleet: FleetStore
    @State private var status: StatusResponse?
    @State private var working: Bool = false
    @State private var error: String?

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                Form {
                    Section("Servers") {
                        TextField("Primary", text: $session.primaryServer)
                            .autocorrectionDisabled()
                            .textInputAutocapitalization(.never)
                            .keyboardType(.URL)
                        TextField("Fallback", text: $session.fallbackServer)
                            .autocorrectionDisabled()
                            .textInputAutocapitalization(.never)
                            .keyboardType(.URL)
                        Button("Test connection") {
                            Task { await test() }
                        }
                        if working {
                            HStack { ProgressView(); Text("Checking…") }
                        }
                        if let err = error {
                            Text(err).foregroundStyle(Theme.crit).font(.caption)
                        }
                    }

                    Section("Fleet status") {
                        if let s = status {
                            row("Hosts", "\(s.active_hosts) / \(s.hosts) up")
                            row("Open alerts", "\(s.open_alerts)")
                            row("Samples stored", "\(s.samples_stored)")
                        } else {
                            Text("—").foregroundStyle(.secondary)
                        }
                        if let ts = fleet.lastUpdate {
                            row("Last refresh", relativeFormatter.localizedString(for: ts, relativeTo: Date()))
                        }
                    }

                    Section("About") {
                        row("Version", "Fleet 0.1 (build dev)")
                        row("Agent compat", "fleet-agent 0.1.x")
                        Link("Open dashboard in browser",
                             destination: URL(string: session.servers.first.map { "\($0)/fleet" } ?? "https://milescoviello.com/fleet") ?? URL(string: "https://example.com")!)
                    }

                    Section {
                        Button(role: .destructive) {
                            session.signOut()
                        } label: {
                            Label("Sign out", systemImage: "rectangle.portrait.and.arrow.right")
                        }
                    }
                }
                .scrollContentBackground(.hidden)
            }
            .navigationTitle("Settings")
            .task { await test() }
        }
    }

    private func row(_ label: String, _ value: String) -> some View {
        HStack { Text(label); Spacer(); Text(value).foregroundStyle(.secondary) }
    }

    private func test() async {
        working = true
        defer { working = false }
        do {
            let s: StatusResponse = try await Network.shared.get("/api/fleet/v1/status", session: session)
            status = s
            error = nil
        } catch {
            self.error = String(describing: error)
        }
    }
}
