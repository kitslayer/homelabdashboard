import Foundation

enum FleetError: LocalizedError {
    case noToken
    case allServersFailed(underlying: [Error])
    case http(status: Int, body: String)

    var errorDescription: String? {
        switch self {
        case .noToken:
            return "Not signed in."
        case .allServersFailed(let errs):
            return "All servers failed: \(errs.first.map { String(describing: $0) } ?? "unknown")"
        case .http(let status, let body):
            return "HTTP \(status): \(body)"
        }
    }
}

actor Network {
    static let shared = Network()

    private lazy var session: URLSession = {
        let cfg = URLSessionConfiguration.ephemeral
        cfg.timeoutIntervalForRequest = 10
        cfg.timeoutIntervalForResource = 20
        cfg.waitsForConnectivity = false
        return URLSession(configuration: cfg)
    }()

    func get<T: Decodable>(_ path: String, session loginSession: Session) async throws -> T {
        try await call(path: path, method: "GET", body: nil as Data?, loginSession: loginSession)
    }

    func post<T: Decodable, B: Encodable>(_ path: String, body: B, session loginSession: Session) async throws -> T {
        let data = try JSONEncoder().encode(body)
        return try await call(path: path, method: "POST", body: data, loginSession: loginSession)
    }

    func patch<T: Decodable, B: Encodable>(_ path: String, body: B, session loginSession: Session) async throws -> T {
        let data = try JSONEncoder().encode(body)
        return try await call(path: path, method: "PATCH", body: data, loginSession: loginSession)
    }

    @discardableResult
    func delete(_ path: String, session loginSession: Session) async throws -> Data {
        let (data, _) = try await raw(path: path, method: "DELETE", body: nil, loginSession: loginSession)
        return data
    }

    private func call<T: Decodable>(path: String, method: String, body: Data?, loginSession: Session) async throws -> T {
        let (data, _) = try await raw(path: path, method: method, body: body, loginSession: loginSession)
        let decoder = JSONDecoder()
        return try decoder.decode(T.self, from: data)
    }

    private func raw(path: String, method: String, body: Data?, loginSession: Session) async throws -> (Data, HTTPURLResponse) {
        let servers = await MainActor.run { loginSession.servers }
        guard let token = await MainActor.run(body: { loginSession.token }) else {
            throw FleetError.noToken
        }
        var errors: [Error] = []
        for server in servers {
            guard var url = URL(string: server) else { continue }
            if let pathURL = URL(string: path, relativeTo: url)?.absoluteURL {
                url = pathURL
            }
            var req = URLRequest(url: url)
            req.httpMethod = method
            req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
            if let body {
                req.httpBody = body
                req.setValue("application/json", forHTTPHeaderField: "Content-Type")
            }
            do {
                let (data, response) = try await session.data(for: req)
                guard let http = response as? HTTPURLResponse else {
                    throw FleetError.http(status: 0, body: "no response")
                }
                if http.statusCode >= 300 {
                    let txt = String(data: data, encoding: .utf8) ?? ""
                    throw FleetError.http(status: http.statusCode, body: txt)
                }
                await MainActor.run { loginSession.rememberWorking(server) }
                return (data, http)
            } catch {
                errors.append(error)
                continue
            }
        }
        throw FleetError.allServersFailed(underlying: errors)
    }
}
