import Foundation

struct Host: Codable, Identifiable, Hashable {
    let id: Int
    let host_uuid: String
    let hostname: String
    let display_name: String?
    let ip: String?
    let tailscale_ip: String?
    let os: String?
    let distro: String?
    let kernel: String?
    let arch: String?
    let agent_version: String?
    let tags: [String]?
    let location_tag: String?
    let lat: Double?
    let lon: Double?
    let floorplan_x: Double?
    let floorplan_y: Double?
    let topology_group: String?
    let notes: String?
    let registered_at: Int
    let last_seen: Int?
    let up: Bool
    let stale_seconds: Int?
    let enabled: Bool
    let latest: Sample?

    var displayName: String { display_name ?? hostname }
}

struct Sample: Codable, Hashable {
    let ts: Int?
    let uptime: Int?
    let cpu: CPUSample?
    let mem: MemSample?
    let disks: [DiskSample]?
    let net: [NetSample]?
    let gpu: [GPUSample]?
    let battery: BatterySample?
    let services: [ServiceSample]?
    let containers: [ContainerSample]?
    let zfs_pools: [ZFSSample]?
    let smart: [SMARTSample]?
    let logs: [LogSample]?

    var rootDisk: DiskSample? {
        disks?.first(where: { $0.mount == "/" })
    }
}

struct CPUSample: Codable, Hashable {
    let pct: Double?
    let cores: Int?
    let per_core: [Double]?
    let load1: Double?
    let load5: Double?
    let load15: Double?
    let freq_mhz: Double?
    let temp: Double?
}

struct MemSample: Codable, Hashable {
    let used: Int64?
    let total: Int64?
    let available: Int64?
    let pct: Double?
    let swap_used: Int64?
    let swap_total: Int64?
}

struct DiskSample: Codable, Hashable {
    let mount: String?
    let device: String?
    let fs: String?
    let used: Int64?
    let total: Int64?
    let pct: Double?
}

struct NetSample: Codable, Hashable {
    let iface: String?
    let rx_bps: Double?
    let tx_bps: Double?
    let rx_total: Int64?
    let tx_total: Int64?
    let speed_mbps: Int?
    let up: Bool?
}

struct GPUSample: Codable, Hashable {
    let name: String?
    let vendor: String?
    let util_pct: Double?
    let mem_used: Int64?
    let mem_total: Int64?
    let temp: Double?
    let power_w: Double?
    let power_limit_w: Double?
    let fan_pct: Double?
    let clock_mhz: Double?
}

struct BatterySample: Codable, Hashable {
    let present: Bool?
    let pct: Double?
    let ac_online: Bool?
    let time_remaining_s: Int?
    let wattage: Double?
}

struct ServiceSample: Codable, Hashable {
    let name: String?
    let status: String?
    let enabled: Bool?
}

struct ContainerSample: Codable, Hashable {
    let name: String?
    let image: String?
    let status: String?
    let cpu_pct: Double?
    let mem_used: Int64?
}

struct ZFSSample: Codable, Hashable {
    let name: String?
    let state: String?
    let cap: Int?
    let free: String?
    let errors: Int?
}

struct SMARTSample: Codable, Hashable {
    let device: String?
    let model: String?
    let temp: Double?
    let hours: Int?
    let health: String?
}

struct LogSample: Codable, Hashable {
    let ts: Int?
    let level: String?
    let unit: String?
    let message: String?
}

struct HostsResponse: Codable {
    let hosts: [Host]
}

struct AlertItem: Codable, Identifiable, Hashable {
    let id: Int
    let host_id: Int?
    let rule_id: Int?
    let severity: String
    let message: String
    let fired_at: Int
    let cleared_at: Int?
    let last_value: Double?
    let hostname: String?
    let display_name: String?
    let rule_name: String?

    var hostLabel: String { display_name ?? hostname ?? "host \(host_id ?? 0)" }
    var isActive: Bool { cleared_at == nil }
}

struct AlertsResponse: Codable {
    let alerts: [AlertItem]
}

struct HistorySeries: Codable {
    let host_id: Int
    let window: String
    let bucket_seconds: Int
    let metrics: [String]
    let points: [HistoryPoint]
}

struct HistoryPoint: Codable, Hashable {
    let ts: Int
    let values: [String: Double?]

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: DynamicKey.self)
        var values: [String: Double?] = [:]
        var ts: Int = 0
        for key in container.allKeys {
            if key.stringValue == "ts" {
                ts = (try? container.decode(Int.self, forKey: key)) ?? 0
            } else if let v = try? container.decodeIfPresent(Double.self, forKey: key) {
                values[key.stringValue] = v
            } else {
                values[key.stringValue] = nil
            }
        }
        self.ts = ts
        self.values = values
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: DynamicKey.self)
        try container.encode(ts, forKey: DynamicKey(stringValue: "ts")!)
        for (k, v) in values {
            if let v {
                try container.encode(v, forKey: DynamicKey(stringValue: k)!)
            }
        }
    }
}

struct DynamicKey: CodingKey, Hashable {
    var stringValue: String
    init?(stringValue: String) { self.stringValue = stringValue }
    var intValue: Int? { nil }
    init?(intValue: Int) { return nil }
}

struct TopologyResponse: Codable {
    struct Node: Codable, Hashable {
        let id: Int
        let label: String
        let group: String?
        let ip: String?
        let tailscale_ip: String?
        let location: String?
        let up: Bool
    }
    struct Edge: Codable, Hashable {
        let id: Int
        let a_host_id: Int
        let b_host_id: Int
        let kind: String
        let label: String?
    }
    let nodes: [Node]
    let edges: [Edge]
}

struct StatusResponse: Codable {
    let hosts: Int
    let active_hosts: Int
    let open_alerts: Int
    let samples_stored: Int
}
