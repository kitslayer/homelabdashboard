import SwiftUI
import MapKit

struct MapView: View {
    @EnvironmentObject var fleet: FleetStore
    @State private var mode: MapMode = .topology

    enum MapMode: String, CaseIterable, Identifiable {
        case topology, geographic, floorplan
        var id: String { rawValue }
        var label: String {
            switch self {
            case .topology: return "Topology"
            case .geographic: return "Geographic"
            case .floorplan: return "Floorplan"
            }
        }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Theme.bg.ignoresSafeArea()
                VStack(spacing: 0) {
                    Picker("View", selection: $mode) {
                        ForEach(MapMode.allCases) { m in Text(m.label).tag(m) }
                    }
                    .pickerStyle(.segmented)
                    .padding(.horizontal, 16)
                    .padding(.top, 8)

                    switch mode {
                    case .topology: TopologyCanvas()
                    case .geographic: GeographicCanvas()
                    case .floorplan: FloorplanCanvas()
                    }
                }
            }
            .navigationTitle("Map")
        }
    }
}

struct TopologyCanvas: View {
    @EnvironmentObject var fleet: FleetStore

    // Simple radial layout: group nodes by topology_group, position each
    // group around a circle, place the nodes within each group as a fan.
    var body: some View {
        GeometryReader { proxy in
            let positions = layout(in: proxy.size)
            ZStack {
                if let topo = fleet.topology {
                    Canvas { ctx, _ in
                        for edge in topo.edges {
                            guard let a = positions[edge.a_host_id], let b = positions[edge.b_host_id] else { continue }
                            var path = Path()
                            path.move(to: a)
                            path.addLine(to: b)
                            ctx.stroke(path, with: .color(Theme.line), lineWidth: 1)
                        }
                    }
                    ForEach(topo.nodes, id: \.id) { node in
                        if let p = positions[node.id] {
                            VStack(spacing: 4) {
                                Circle()
                                    .fill(node.up ? Theme.ok : Theme.crit)
                                    .frame(width: 24, height: 24)
                                    .overlay(Circle().stroke(Color.black, lineWidth: 2))
                                Text(node.label)
                                    .font(.caption2)
                                    .foregroundStyle(.primary)
                                    .lineLimit(1)
                            }
                            .position(p)
                            .onTapGesture {
                                NotificationCenter.default.post(name: .openHost, object: node.id)
                            }
                        }
                    }
                } else {
                    ProgressView()
                }
            }
        }
    }

    private func layout(in size: CGSize) -> [Int: CGPoint] {
        guard let topo = fleet.topology, !topo.nodes.isEmpty else { return [:] }
        let groups = Dictionary(grouping: topo.nodes, by: { $0.group ?? "host" })
        let groupKeys = groups.keys.sorted()
        let center = CGPoint(x: size.width / 2, y: size.height / 2)
        let radius = min(size.width, size.height) * 0.35
        var out: [Int: CGPoint] = [:]
        for (gi, gkey) in groupKeys.enumerated() {
            let nodes = groups[gkey] ?? []
            let groupAngle = 2 * .pi * Double(gi) / Double(groupKeys.count)
            let groupCenter = CGPoint(
                x: center.x + radius * cos(groupAngle),
                y: center.y + radius * sin(groupAngle)
            )
            let inner = min(60, 14 + 16 * Double(nodes.count))
            for (ni, node) in nodes.enumerated() {
                let nodeAngle = groupAngle + (2 * .pi / Double(max(groupKeys.count, 1))) * (Double(ni) / Double(max(nodes.count, 1)) - 0.5) * 0.6
                let p = CGPoint(
                    x: groupCenter.x + inner * cos(nodeAngle),
                    y: groupCenter.y + inner * sin(nodeAngle)
                )
                out[node.id] = p
            }
        }
        return out
    }
}

struct GeographicCanvas: View {
    @EnvironmentObject var fleet: FleetStore
    @State private var position: MapCameraPosition = .automatic

    var pinHosts: [Host] {
        fleet.hosts.filter { $0.lat != nil && $0.lon != nil }
    }

    var body: some View {
        Group {
            if pinHosts.isEmpty {
                VStack(spacing: 14) {
                    Image(systemName: "mappin.slash").font(.largeTitle).foregroundStyle(.secondary)
                    Text("No hosts have lat/lon set yet.")
                        .multilineTextAlignment(.center)
                        .foregroundStyle(.secondary)
                    Text("Open a host and set Geo coordinates to pin it here.")
                        .font(.caption)
                        .multilineTextAlignment(.center)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .padding(40)
            } else {
                Map(position: $position) {
                    ForEach(pinHosts) { host in
                        Annotation(host.displayName, coordinate: CLLocationCoordinate2D(latitude: host.lat!, longitude: host.lon!)) {
                            Circle()
                                .fill(host.up ? Theme.ok : Theme.crit)
                                .frame(width: 18, height: 18)
                                .overlay(Circle().stroke(Color.white, lineWidth: 2))
                                .onTapGesture {
                                    NotificationCenter.default.post(name: .openHost, object: host.id)
                                }
                        }
                    }
                }
                .mapStyle(.standard(elevation: .flat, pointsOfInterest: .excludingAll))
            }
        }
    }
}

struct FloorplanCanvas: View {
    @EnvironmentObject var fleet: FleetStore
    @State private var draggedID: Int? = nil

    var body: some View {
        GeometryReader { proxy in
            ZStack {
                Color.black.opacity(0.2)
                ForEach(fleet.hosts) { host in
                    let x = (host.floorplan_x ?? Double((host.id * 37) % 100) / 100) * Double(proxy.size.width)
                    let y = (host.floorplan_y ?? Double((host.id * 73) % 100) / 100) * Double(proxy.size.height)
                    let pos = CGPoint(x: x, y: y)
                    FloorplanPin(host: host)
                        .position(pos)
                        .gesture(DragGesture()
                            .onChanged { _ in draggedID = host.id }
                            .onEnded { value in
                                let nx = min(max(0, value.location.x / proxy.size.width), 1)
                                let ny = min(max(0, value.location.y / proxy.size.height), 1)
                                Task {
                                    try? await fleet.updateHost(id: host.id, patch: [
                                        "floorplan_x": AnyEncodable(nx),
                                        "floorplan_y": AnyEncodable(ny),
                                    ])
                                }
                                draggedID = nil
                            })
                }
            }
        }
        .padding(8)
    }
}

struct FloorplanPin: View {
    let host: Host
    var body: some View {
        VStack(spacing: 3) {
            Circle()
                .fill(host.up ? Theme.ok : Theme.crit)
                .frame(width: 18, height: 18)
                .overlay(Circle().stroke(Color.white, lineWidth: 2))
            Text(host.displayName)
                .font(.caption2)
                .padding(.horizontal, 5).padding(.vertical, 2)
                .background(Capsule().fill(Color.black.opacity(0.55)))
                .lineLimit(1)
        }
    }
}

extension Notification.Name {
    static let openHost = Notification.Name("fleet.openHost")
}
