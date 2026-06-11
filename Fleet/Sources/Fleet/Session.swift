import Foundation
import SwiftUI

@MainActor
final class Session: ObservableObject {
    // Stored bits are persisted via UserDefaults; in a real app the token would
    // live in Keychain. UserDefaults keeps the sample short while still
    // surviving relaunches.
    @AppStorage("fleet.server.primary") var primaryServer: String = "http://192.0.2.10"
    @AppStorage("fleet.server.fallback") var fallbackServer: String = "https://milescoviello.com"
    @AppStorage("fleet.token") private var storedToken: String = ""
    @AppStorage("fleet.lastWorking") private var lastWorking: String = ""

    @Published var token: String? = nil
    @Published var statusError: String? = nil

    init() {
        if !storedToken.isEmpty {
            token = storedToken
        }
    }

    var servers: [String] {
        var out: [String] = []
        if !lastWorking.isEmpty { out.append(lastWorking) }
        for s in [primaryServer, fallbackServer] {
            let trimmed = s.trimmingCharacters(in: .whitespaces)
            if !trimmed.isEmpty && !out.contains(trimmed) {
                out.append(trimmed)
            }
        }
        return out
    }

    func signIn(_ token: String) async -> Bool {
        let trimmed = token.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return false }
        self.token = trimmed
        self.storedToken = trimmed
        do {
            let _: StatusResponse = try await Network.shared.get("/api/fleet/v1/status", session: self)
            statusError = nil
            return true
        } catch {
            self.token = nil
            self.storedToken = ""
            statusError = error.localizedDescription
            return false
        }
    }

    func signOut() {
        token = nil
        storedToken = ""
        lastWorking = ""
    }

    func rememberWorking(_ s: String) {
        lastWorking = s
    }
}
