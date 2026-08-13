import SwiftUI
import CoreText

@main
struct FleetApp: App {
    @StateObject private var store = ArgusStore()

    init() {
        // register bundled Chakra Petch faces (display font)
        if let urls = Bundle.module.urls(forResourcesWithExtension: "ttf", subdirectory: "Fonts") {
            for u in urls { CTFontManagerRegisterFontsForURL(u as CFURL, .process, nil) }
        }
    }

    var body: some Scene {
        WindowGroup {
            RootShell()
                .environmentObject(store)
                .preferredColorScheme(.dark)
                .tint(Theme.amber)
                .onAppear { store.start() }
        }
    }
}

// ── tabs (mirror argus app.js TABS) ──────────────────────────────────
enum Tab: String, CaseIterable, Identifiable {
    case overview, hosts, services, pve, docker, k8s, storage, ollama, hermes, network
    var id: String { rawValue }
    var title: String {
        switch self {
        case .overview: return "OVERVIEW"
        case .hosts:    return "HOSTS"
        case .services: return "SERVICES"
        case .pve:      return "PROXMOX"
        case .docker:   return "DOCKER"
        case .k8s:      return "K3S"
        case .storage:  return "STORAGE"
        case .ollama:   return "OLLAMA"
        case .hermes:   return "HERMES"
        case .network:  return "NETWORK"
        }
    }
    var icon: String {
        switch self {
        case .overview: return "eye"
        case .hosts:    return "square.grid.2x2"
        case .services: return "waveform.path.ecg"
        case .pve:      return "cube.transparent"
        case .docker:   return "shippingbox"
        case .k8s:      return "circle.hexagongrid"
        case .storage:  return "externaldrive"
        case .ollama:   return "cpu"
        case .hermes:   return "bubble.left.and.bubble.right"
        case .network:  return "network"
        }
    }
}

struct RootShell: View {
    @EnvironmentObject var store: ArgusStore
    @State private var tab: Tab = .overview
    @State private var showSettings = false

    var body: some View {
        ZStack {
            AtmosphereBackground()
            VStack(spacing: 0) {
                TopBar(tab: tab, showSettings: $showSettings)
                TabRail(selected: $tab)
                Divider().overlay(Theme.edge)
                if store.lastError != nil {
                    HStack(spacing: 7) {
                        Image(systemName: "wifi.slash").font(.system(size: 10))
                        Text("Argus unreachable — showing last data").font(Theme.mono(10.5))
                        Spacer()
                    }
                    .foregroundStyle(Theme.red)
                    .padding(.horizontal, 16).padding(.vertical, 6)
                    .background(Theme.red.opacity(0.10))
                }
                content
            }
        }
        .sheet(isPresented: $showSettings) { SettingsView() }
    }

    @ViewBuilder private var content: some View {
        switch tab {
        case .overview: OverviewView(go: { tab = $0 })
        case .hosts:    HostsView()
        case .services: ServicesView()
        case .pve:      ProxmoxView()
        case .docker:   DockerView()
        case .k8s:      K8sView()
        case .storage:  StorageView()
        case .ollama:   OllamaView()
        case .hermes:   HermesView()
        case .network:  NetworkView()
        }
    }
}

// ── topbar: title • clock • WAN/alert pills ──────────────────────────
struct TopBar: View {
    @EnvironmentObject var store: ArgusStore
    var tab: Tab
    @Binding var showSettings: Bool

    var body: some View {
        HStack(alignment: .center, spacing: 12) {
            VStack(alignment: .leading, spacing: 3) {
                Text(tab.title).font(Theme.display(21)).tracking(2).foregroundStyle(Theme.ink)
                Rectangle().fill(Theme.amber).frame(width: 34, height: 2)
            }
            Spacer()
            statusPills
            TimelineView(.periodic(from: .now, by: 1)) { ctx in
                Text(clock(ctx.date))
                    .font(Theme.mono(18, .medium)).foregroundStyle(Theme.amber)
            }
            Button { showSettings = true } label: {
                Image(systemName: "gearshape").foregroundStyle(Theme.inkDim)
            }
        }
        .padding(.horizontal, 18).padding(.top, 10).padding(.bottom, 10)
        .background(LinearGradient(colors: [Theme.amber.opacity(0.025), .clear], startPoint: .top, endPoint: .bottom))
    }

    private var statusPills: some View {
        HStack(spacing: 10) {
            let wanUp = store.overview?.wan?.up ?? false
            pill(dot: wanUp ? Theme.green : Theme.red,
                 text: wanUp ? String(format: "%.0fms", store.overview?.wan?.ms ?? 0) : "WAN")
            let alerts = store.alertCount
            pill(dot: alerts == 0 ? Theme.green : Theme.red,
                 text: alerts == 0 ? "OK" : "\(alerts)")
        }
    }

    private func pill(dot: Color, text: String) -> some View {
        HStack(spacing: 7) {
            Circle().fill(dot).frame(width: 7, height: 7)
            Text(text).font(Theme.mono(11)).foregroundStyle(Theme.inkDim)
        }
        .padding(.horizontal, 11).padding(.vertical, 7)
        .background(Theme.panel).clipShape(Chamfer(cut: 8))
        .overlay(Chamfer(cut: 8).stroke(Theme.line, lineWidth: 1))
    }

    private func clock(_ d: Date) -> String {
        let f = DateFormatter(); f.dateFormat = "HH:mm:ss"; return f.string(from: d)
    }
}

// ── horizontal tab rail ──────────────────────────────────────────────
struct TabRail: View {
    @EnvironmentObject var store: ArgusStore
    @Binding var selected: Tab

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 0) {
                ForEach(Tab.allCases) { t in
                    Button { selected = t } label: {
                        VStack(spacing: 5) {
                            ZStack(alignment: .topTrailing) {
                                Image(systemName: t.icon).font(.system(size: 18))
                                if t == .overview, store.alertCount > 0 {
                                    Circle().fill(Theme.red).frame(width: 7, height: 7).offset(x: 6, y: -3)
                                }
                            }
                            Text(t.title).font(Theme.display(9)).tracking(1)
                        }
                        .foregroundStyle(selected == t ? Theme.amber : Theme.inkFaint)
                        .frame(minWidth: 62)
                        .padding(.vertical, 9)
                        .overlay(alignment: .bottom) {
                            if selected == t {
                                Rectangle().fill(Theme.amber).frame(height: 2)
                            }
                        }
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.horizontal, 8)
        }
        .background(Theme.bgRaise.opacity(0.6))
    }
}

// ── atmosphere: graph-paper grid + corner glows ──────────────────────
struct AtmosphereBackground: View {
    var body: some View {
        ZStack {
            Theme.bg
            Canvas { ctx, size in
                let step: CGFloat = 64
                var path = Path()
                var x: CGFloat = 0
                while x < size.width { path.move(to: CGPoint(x: x, y: 0)); path.addLine(to: CGPoint(x: x, y: size.height)); x += step }
                var y: CGFloat = 0
                while y < size.height { path.move(to: CGPoint(x: 0, y: y)); path.addLine(to: CGPoint(x: size.width, y: y)); y += step }
                ctx.stroke(path, with: .color(Color(red: 0.55, green: 0.62, blue: 0.71, opacity: 0.028)), lineWidth: 1)
            }
        }
        .ignoresSafeArea()
    }
}
