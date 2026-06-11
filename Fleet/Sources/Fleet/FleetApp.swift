import SwiftUI

@main
struct FleetApp: App {
    @StateObject private var session = Session()
    @StateObject private var fleet = FleetStore()

    var body: some Scene {
        WindowGroup {
            RootView()
                .environmentObject(session)
                .environmentObject(fleet)
                .onAppear {
                    fleet.session = session
                    Task { await fleet.poll() }
                }
        }
    }
}

struct RootView: View {
    @EnvironmentObject var session: Session
    @EnvironmentObject var fleet: FleetStore

    var body: some View {
        Group {
            if session.token == nil {
                LoginView()
            } else {
                MainTabView()
            }
        }
        .preferredColorScheme(.dark)
        .tint(Theme.accent)
    }
}

struct MainTabView: View {
    @EnvironmentObject var fleet: FleetStore

    var body: some View {
        TabView {
            DashboardView()
                .tabItem { Label("Hosts", systemImage: "rectangle.grid.2x2.fill") }
            MapView()
                .tabItem { Label("Map", systemImage: "globe.americas.fill") }
            AlertsView()
                .tabItem { Label("Alerts", systemImage: "bell.fill") }
                .badge(fleet.activeAlerts.count == 0 ? nil : Text(String(fleet.activeAlerts.count)))
            SettingsView()
                .tabItem { Label("Settings", systemImage: "gearshape.fill") }
        }
    }
}
