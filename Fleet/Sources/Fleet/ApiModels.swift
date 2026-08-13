import Foundation

// ════════════════════════════════════════════════════════════════════
//  Per-section API response models (match argus/api.go handlers).
//  All fields optional/loose so partial/erroring endpoints still decode.
// ════════════════════════════════════════════════════════════════════

// MARK: - Services  (/api/services)

struct ServicesResponse: Codable {
    var services: [Service]?
    var minecraft: [MCServer]?
}
struct Service: Codable, Identifiable {
    var id: String
    var name: String
    var group: String?
    var url: String?
    var up: Bool = false
    var code: Int?
    var ms: Double?
    var since: Int?
    var fails_in_row: Int?
    var ok_count: Int?
    var total: Int?
}
struct MCServer: Codable, Identifiable {
    var name: String
    var addr: String?
    var up: Bool = false
    var version: String?
    var online: Int?
    var max: Int?
    var motd: String?
    var id: String { name }
}

// MARK: - Network  (/api/network)

struct NetworkResponse: Codable {
    var pings: [String: Ping]?
    var wan: [WanPoint]?
}
struct Ping: Codable {
    var up: Bool = false
    var ms: Double?
    var active_ip: String?
    var last_seen: Int?
    var since: Int?
}
struct WanPoint: Codable {   // all optional — keys may vary
    var up: Bool?
    var ms: Double?
    var ts: Int?
}

// MARK: - Proxmox  (/api/pve)

struct PVEResponse: Codable {
    var state: PVEState?
    var ts: Int?
    var err: String?
}
struct PVEState: Codable {
    var nodes: [PVENode]?
    var vms: [PVEVM]?
}
struct PVENode: Codable, Identifiable {
    var node: String
    var status: String?
    var cpu: Double?
    var maxcpu: Double?
    var mem: Double?
    var maxmem: Double?
    var disk: Double?
    var maxdisk: Double?
    var uptime: Int?
    var id: String { node }
    var cpuPct: Double? { (cpu).map { $0 * 100 } }
    var memPct: Double? { guard let m = mem, let mx = maxmem, mx > 0 else { return nil }; return m / mx * 100 }
    var diskPct: Double? { guard let d = disk, let mx = maxdisk, mx > 0 else { return nil }; return d / mx * 100 }
}
struct PVEVM: Codable, Identifiable {
    var node: String?
    var vmid: Int
    var name: String?
    var status: String?
    var type: String?
    var cpu: Double?
    var maxcpu: Double?
    var mem: Double?
    var maxmem: Double?
    var uptime: Int?
    var lock: String?
    var id: Int { vmid }
}

// MARK: - K3s  (/api/k8s)

struct K8sResponse: Codable {
    var state: K8sState?
    var ts: Int?
    var err: String?
}
struct K8sState: Codable {
    var nodes: [K8sNode]?
    var pods: [K8sPod]?
    var workloads: [K8sWorkload]?
}
struct K8sWorkload: Codable, Identifiable {
    var kind: String?
    var name: String
    var ns: String?
    var ready: Int?
    var desired: Int?
    var id: String { (ns ?? "") + "/" + (kind ?? "") + "/" + name }
}
struct K8sNode: Codable, Identifiable {
    var name: String
    var ready: Bool = false
    var version: String?
    var ip: String?
    var id: String { name }
}
struct K8sPod: Codable, Identifiable {
    var name: String
    var ns: String?
    var phase: String?
    var ready: String?
    var restarts: Int?
    var node: String?
    var start_ts: Int?
    var id: String { (ns ?? "") + "/" + name }
}

// MARK: - Docker  (/api/docker)

struct DockerResponse: Codable {
    var targets: [IDName]?
    var containers: [String: [DockerContainer]]?
    var ts: Int?
    var err: String?
}
struct DockerContainer: Codable, Identifiable {
    var id: String
    var name: String
    var image: String?
    var state: String?
    var status: String?
}
struct DockerStats: Codable { var cpu_pct: Double?; var mem_display: String? }

// MARK: - Storage / TrueNAS  (/api/storage)

struct StorageResponse: Codable {
    var truenas: TrueNAS?
    var ts: Int?
    var err: String?
}
struct TrueNAS: Codable {
    var pools: [TNPool]?
    var datasets: [TNDataset]?
}
struct TNPool: Codable, Identifiable {
    var name: String?
    var size: String?       // human strings from zpool list ("38.2T")
    var alloc: String?
    var free: String?
    var cap_pct: Double?    // int in JSON, decodes to Double
    var frag: String?
    var health: String?
    var id: String { name ?? UUID().uuidString }
}
struct TNDataset: Codable, Identifiable {
    var name: String?
    var used: String?
    var avail: String?
    var mount: String?
    var id: String { name ?? UUID().uuidString }
}

// MARK: - Ollama  (/api/ollama)

struct OllamaResponse: Codable {
    var targets: [OllamaTarget]?
}
struct OllamaTarget: Codable, Identifiable {
    var id: String
    var name: String
    var url: String?
    var tags: OllamaTags?
    var ps: OllamaPS?
    var err: String?
}
struct OllamaTags: Codable {
    var models: [OllamaModel]?
}
struct OllamaModel: Codable, Identifiable {
    var name: String
    var model: String?
    var size: Double?
    var details: OllamaDetails?
    var id: String { name }
}
struct OllamaDetails: Codable {
    var parameter_size: String?
    var quantization_level: String?
    var family: String?
}
struct OllamaPS: Codable {
    var models: [OllamaPSModel]?
}
struct OllamaPSModel: Codable, Identifiable {
    var name: String
    var size: Double?
    var size_vram: Double?
    var id: String { name }
}

// MARK: - Hermes  (/api/hermes)

struct HermesResponse: Codable {
    var status: HermesStatus?
    var ts: Int?
    var err: String?
}

// MARK: - Host detail  (/api/hosts/{name})

struct HostDetailResponse: Codable {
    var host: HostView
    var latest: LatestSample?
}
struct LatestSample: Codable {
    var disks: [DiskInfo]?
    var smart: [SmartInfo]?
}
struct DiskInfo: Codable, Identifiable {
    var mount: String
    var device: String?
    var fs: String?
    var used: Double?
    var total: Double?
    var pct: Double?
    var id: String { mount }
}
struct SmartInfo: Codable, Identifiable {
    var device: String
    var model: String?
    var health: String?
    var temp: Double?
    var hours: Int?
    var reallocated: Int?
    var id: String { device }
}

// MARK: - store fetch helpers

extension ArgusStore {
    func services() async throws -> ServicesResponse { try await api.get("/api/services", as: ServicesResponse.self) }
    func network() async throws -> NetworkResponse  { try await api.get("/api/network", as: NetworkResponse.self) }
    func pve() async throws -> PVEResponse          { try await api.get("/api/pve", as: PVEResponse.self) }
    func k8s() async throws -> K8sResponse          { try await api.get("/api/k8s", as: K8sResponse.self) }
    func docker() async throws -> DockerResponse    { try await api.get("/api/docker", as: DockerResponse.self) }
    func storage() async throws -> StorageResponse  { try await api.get("/api/storage", as: StorageResponse.self) }
    func ollama() async throws -> OllamaResponse    { try await api.get("/api/ollama", as: OllamaResponse.self) }
    func hostDetail(_ name: String) async throws -> HostDetailResponse {
        try await api.get("/api/hosts/\(name)", as: HostDetailResponse.self)
    }

    // actions (mutating — api.post sends the X-Argus-Confirm header)
    func hostPower(host: String, op: String) async throws { _ = try await api.post("/api/power", body: ["host": host, "op": op]) }
    func pvePower(vmid: Int, op: String) async throws { _ = try await api.post("/api/pve/power", body: ["vmid": vmid, "op": op]) }
    func dockerAction(target: String, name: String, op: String) async throws { _ = try await api.post("/api/docker/\(target)/\(name)/\(op)") }
    func dockerStats(target: String, name: String) async throws -> DockerStats { try await api.get("/api/docker/\(target)/\(name)/stats", as: DockerStats.self) }
    func k8sDeletePod(ns: String, name: String) async throws { _ = try await api.post("/api/k8s/delete-pod", body: ["ns": ns, "name": name]) }
    func k8sRestart(kind: String, ns: String, name: String) async throws { _ = try await api.post("/api/k8s/restart", body: ["kind": kind, "ns": ns, "name": name]) }
    func ollamaUnload(target: String, model: String) async throws { _ = try await api.post("/api/ollama/unload", body: ["target": target, "model": model]) }
    func binpkgBuild() async throws { _ = try await api.post("/api/tools/binpkg") }
}
