import Foundation
import SwiftUI
import UserNotifications

@MainActor
final class FleetStore: ObservableObject {
    weak var session: Session?

    @Published var hosts: [Host] = []
    @Published var topology: TopologyResponse?
    @Published var alerts: [AlertItem] = []
    @Published var lastUpdate: Date?
    @Published var lastError: String?
    @Published var isLoading: Bool = false

    private var seenAlertIDs: Set<Int> = []
    private var pollTask: Task<Void, Never>?

    var activeAlerts: [AlertItem] { alerts.filter { $0.isActive } }

    func start() {
        Task { await poll() }
    }

    func poll() async {
        pollTask?.cancel()
        pollTask = Task { [weak self] in
            await self?.requestNotificationPermissionIfNeeded()
            while !Task.isCancelled {
                await self?.refresh()
                try? await Task.sleep(nanoseconds: 15_000_000_000)
            }
        }
    }

    func refresh() async {
        guard let session = session, session.token != nil else { return }
        isLoading = true
        defer { isLoading = false }
        do {
            async let h: HostsResponse = Network.shared.get("/api/fleet/v1/hosts", session: session)
            async let t: TopologyResponse = Network.shared.get("/api/fleet/v1/topology", session: session)
            async let a: AlertsResponse = Network.shared.get("/api/fleet/v1/alerts?include_cleared=true&limit=200", session: session)
            let (hostsResp, topo, alertsResp) = try await (h, t, a)
            self.hosts = hostsResp.hosts
            self.topology = topo
            self.alerts = alertsResp.alerts
            self.lastUpdate = Date()
            self.lastError = nil
            await postNotificationsForNewAlerts()
        } catch {
            self.lastError = String(describing: error)
        }
    }

    private func requestNotificationPermissionIfNeeded() async {
        let center = UNUserNotificationCenter.current()
        let settings = await center.notificationSettings()
        if settings.authorizationStatus == .notDetermined {
            _ = try? await center.requestAuthorization(options: [.alert, .badge, .sound])
        }
    }

    private func postNotificationsForNewAlerts() async {
        let center = UNUserNotificationCenter.current()
        for alert in alerts where alert.isActive {
            guard !seenAlertIDs.contains(alert.id) else { continue }
            seenAlertIDs.insert(alert.id)
            let content = UNMutableNotificationContent()
            content.title = "\(alert.severity.capitalized): \(alert.rule_name ?? "alert")"
            content.body = alert.message
            content.sound = alert.severity == "critical" ? .defaultCritical : .default
            content.userInfo = ["alert_id": alert.id, "host_id": alert.host_id as Any]
            let req = UNNotificationRequest(identifier: "fleet-\(alert.id)", content: content, trigger: nil)
            try? await center.add(req)
        }
        // garbage-collect ids that have cleared so a re-trigger can re-notify
        let activeIDs = Set(alerts.filter { $0.isActive }.map(\.id))
        seenAlertIDs.formIntersection(activeIDs)
    }

    // Detail fetches for views that need fresh data.

    func host(id: Int) -> Host? {
        hosts.first(where: { $0.id == id })
    }

    func fetchHistory(hostID: Int, window: String) async throws -> HistorySeries {
        guard let session = session else { throw FleetError.noToken }
        return try await Network.shared.get("/api/fleet/v1/hosts/\(hostID)/history?window=\(window)", session: session)
    }

    func updateHost(id: Int, patch: [String: AnyEncodable]) async throws {
        guard let session = session else { throw FleetError.noToken }
        let _: Host = try await Network.shared.patch("/api/fleet/v1/hosts/\(id)", body: patch, session: session)
        await refresh()
    }
}

struct AnyEncodable: Encodable {
    let value: Encodable?

    init(_ value: Encodable?) { self.value = value }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch value {
        case let v as Int: try container.encode(v)
        case let v as Double: try container.encode(v)
        case let v as String: try container.encode(v)
        case let v as Bool: try container.encode(v)
        case let v as [String: AnyEncodable]: try container.encode(v)
        case let v as [AnyEncodable]: try container.encode(v)
        case nil: try container.encodeNil()
        default:
            if let v = value { try v.encode(to: encoder) }
            else { try container.encodeNil() }
        }
    }
}
