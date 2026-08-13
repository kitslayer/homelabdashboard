import Foundation
import SwiftUI

// ════════════════════════════════════════════════════════════════════
//  Argus API — models, client, polling store
//  Backend: the Argus Go server (default http://192.168.1.195:8800)
//  GET is open on the LAN; mutating POSTs need header X-Argus-Confirm.
// ════════════════════════════════════════════════════════════════════

// MARK: - Models (match argus/api.go JSON)

struct Overview: Codable {
    var ts: Int?
    var wan: WAN?
    var hosts: [HostView] = []
    var alerts: Alerts?
    var k3s: K3sSummary?
    var docker: [String: DockerCount]?
    var pools: [Pool]?
    var hermes: HermesStatus?
    var services: Count?
    var weather: Weather?
}

struct WAN: Codable { var up: Bool = false; var ms: Double = 0 }
struct Count: Codable { var up: Int = 0; var total: Int = 0 }
struct DockerCount: Codable { var up: Int = 0; var total: Int = 0 }

struct Alerts: Codable {
    var fleet: [FleetAlert]?
    var fleet_err: String?
    var synthetic: [SyntheticAlert]?
}

struct FleetAlert: Codable, Identifiable {
    var id: Int
    var severity: String
    var message: String
    var fired_at: Int?
    var count: Int?
    var hostname: String?
    var display_name: String?
}

struct SyntheticAlert: Codable, Identifiable {
    var id = UUID()
    var severity: String
    var message: String
    var tab: String?
    enum CodingKeys: String, CodingKey { case severity, message, tab }
}

struct K3sSummary: Codable {
    var nodes_ready: Int?
    var nodes_total: Int?
    var pods_running: Int?
    var pods_bad: Int?
}

struct Pool: Codable, Identifiable {
    var name: String
    var health: String
    var cap_pct: Double?
    var id: String { name }
}

struct HermesStatus: Codable {
    var gateway_active: Bool?
    var version: String?
    var mode: String?
    var http_healthy: Bool?
}

struct Weather: Codable {
    var temp_c: Double?
    var feels_c: Double?
    var humidity: Int?
    var wind_kmh: Double?
    var code: Int?
    var desc: String?
    var today_max_c: Double?
    var today_min_c: Double?
    var fetched_at: Int?
}

// host (overview + detail share this shape)
struct HostView: Codable, Identifiable {
    var name: String
    var label: String
    var ip: String?
    var kind: String?
    var group: String?
    var note: String?
    var caps: [String]?
    var up: Bool = false
    var source: String?
    var ping_ms: Double?
    var since: Int?
    var fleet_id: Int?
    var fleet: FleetMetrics?
    var pve: PVEInfo?
    var can_wake: Bool?
    var can_power: Bool?
    var id: String { name }

    var groupKey: String { group ?? "edge" }
}

struct FleetMetrics: Codable {
    var cpu_pct: Double?
    var cpu_temp: Double?
    var load1: Double?
    var cores: Int?
    var mem_pct: Double?
    var mem_used: Double?
    var mem_total: Double?
    var uptime: Int?
    var kernel: String?
    var distro: String?
    var disk_root_pct: Double?
    var disk_max_pct: Double?
    var disk_max_mount: String?
    var net_rx_bps: Double?
    var net_tx_bps: Double?
    var gpu: [GPU]?
    var battery: Battery?
}

struct GPU: Codable {
    var name: String?
    var util_pct: Double?
    var temp: Double?
    var mem_used: Double?
    var mem_total: Double?
    var power_w: Double?
}

struct Battery: Codable {
    var ac_online: Bool?
    var pct: Double?
    var present: Bool?
}

struct PVEInfo: Codable {
    var node: String?
    var vmid: Int?
    var status: String?
    var type: String?
    var uptime: Int?
}

// ui-config
struct UIConfig: Codable {
    var hosts: [UIHost]?
    var ollama: [IDName]?
    var docker: [IDName]?
    var binhost_url: String?
    var k8s: Bool?
    var version: String?
    var hermes_prompts: [String]?
}
struct IDName: Codable, Identifiable { var id: String; var name: String }
struct UIHost: Codable {
    var name: String
    var label: String?
    var ip: String?
    var group: String?
    var caps: [String]?
    var has_fleet: Bool?
    var fleet_hostname: String?
}

// host groups (mirror ui.js GROUP_ORDER / GROUP_NAMES)
enum HostGroup {
    static let order = ["core", "pve", "edge", "remote"]
    static func name(_ g: String) -> String {
        switch g {
        case "core":   return "CORE SYSTEMS"
        case "pve":    return "PROXMOX CLUSTER"
        case "edge":   return "EDGE / GUESTS"
        case "remote": return "REMOTE"
        default:       return g.uppercased()
        }
    }
}

// MARK: - API client

enum APIError: LocalizedError {
    case badURL, http(Int), message(String)
    var errorDescription: String? {
        switch self {
        case .badURL: return "bad URL"
        case .http(let c): return "HTTP \(c)"
        case .message(let m): return m
        }
    }
}

struct ArgusAPI {
    var base: String

    private func url(_ path: String) throws -> URL {
        guard let u = URL(string: base + path) else { throw APIError.badURL }
        return u
    }

    func get<T: Decodable>(_ path: String, as: T.Type) async throws -> T {
        var req = URLRequest(url: try url(path))
        req.timeoutInterval = 12
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let h = resp as? HTTPURLResponse, !(200..<300).contains(h.statusCode) {
            throw APIError.http(h.statusCode)
        }
        return try JSONDecoder().decode(T.self, from: data)
    }

    /// mutating POST — sends the confirm header the backend requires for actions.
    /// Returns Void (Sendable); throws APIError.message with the server error on failure.
    func post(_ path: String, body: [String: any Sendable] = [:]) async throws {
        var req = URLRequest(url: try url(path))
        req.httpMethod = "POST"
        req.timeoutInterval = 20
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.setValue("argus-ui", forHTTPHeaderField: "X-Argus-Confirm")
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (data, resp) = try await URLSession.shared.data(for: req)
        if let h = resp as? HTTPURLResponse, !(200..<300).contains(h.statusCode) {
            let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
            throw APIError.message((obj?["error"] as? String) ?? "HTTP \(h.statusCode)")
        }
    }
}

// MARK: - Store

@MainActor
final class ArgusStore: ObservableObject {
    @Published var overview: Overview?
    @Published var uiConfig: UIConfig?
    @Published var lastError: String?
    @Published var lastUpdate: Date?
    @Published var base: String {
        didSet { UserDefaults.standard.set(base, forKey: "argus.base") }
    }

    var api: ArgusAPI { ArgusAPI(base: base) }
    private var task: Task<Void, Never>?

    init() {
        self.base = UserDefaults.standard.string(forKey: "argus.base") ?? "http://192.168.1.195:8800"
    }

    func start() {
        guard task == nil else { return }
        task = Task { [weak self] in
            while !Task.isCancelled {
                await self?.pollOnce()
                try? await Task.sleep(nanoseconds: 5_000_000_000)
            }
        }
    }

    func pollOnce() async {
        do {
            if uiConfig == nil { uiConfig = try? await api.get("/api/ui-config", as: UIConfig.self) }
            let ov = try await api.get("/api/overview", as: Overview.self)
            self.overview = ov
            self.lastUpdate = Date()
            self.lastError = nil
        } catch {
            self.lastError = error.localizedDescription
        }
    }

    func refresh() async { await pollOnce() }

    // derived
    var hosts: [HostView] { overview?.hosts ?? [] }
    var alertCount: Int {
        (overview?.alerts?.fleet?.count ?? 0) + (overview?.alerts?.synthetic?.count ?? 0)
    }
    var hasCritical: Bool {
        (overview?.alerts?.fleet?.contains { $0.severity == "critical" } ?? false) ||
        (overview?.alerts?.synthetic?.contains { $0.severity == "critical" } ?? false)
    }
}

// MARK: - formatters (mirror fmt.js)

enum Fmt {
    static func pct(_ p: Double?) -> String {
        guard let p else { return "—" }
        return String(format: "%.0f%%", p)
    }
    static func temp(_ t: Double?) -> String {
        guard let t else { return "—" }
        return String(format: "%.0f°C", t)
    }
    static func bytes(_ n: Double?) -> String {
        guard let n else { return "—" }
        let units = ["B", "KB", "MB", "GB", "TB", "PB"]
        var v = n, i = 0
        while v >= 1024 && i < units.count - 1 { v /= 1024; i += 1 }
        return String(format: (v >= 10 || i == 0) ? "%.0f %@" : "%.1f %@", v, units[i])
    }
    static func bps(_ n: Double?) -> String {
        guard let n else { return "—" }
        return bytes(n) + "/s"
    }
    static func uptime(_ s: Int?) -> String {
        guard let s else { return "—" }
        let d = s / 86400, h = (s % 86400) / 3600, m = (s % 3600) / 60
        if d > 0 { return "\(d)d \(h)h" }
        if h > 0 { return "\(h)h \(m)m" }
        return "\(m)m"
    }
    static func ago(_ ts: Int?) -> String {
        guard let ts, ts > 0 else { return "" }
        let secs = Int(Date().timeIntervalSince1970) - ts
        if secs < 60 { return "\(max(secs, 0))s ago" }
        if secs < 3600 { return "\(secs / 60)m ago" }
        if secs < 86400 { return "\(secs / 3600)h ago" }
        return "\(secs / 86400)d ago"
    }
}
