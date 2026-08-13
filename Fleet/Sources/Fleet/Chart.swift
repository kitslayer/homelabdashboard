import SwiftUI

// lenient numeric decode: accepts number | null | missing
struct OptNum: Codable {
    var v: Double?
    init(from d: Decoder) throws { let c = try d.singleValueContainer(); v = try? c.decode(Double.self) }
}

struct HistoryResponse: Codable {
    var window: String?
    var bucket_seconds: Int?
    var metrics: [String]?
    var points: [[String: OptNum]]?
}

extension ArgusStore {
    func history(_ name: String, window: String, metric: String) async throws -> HistoryResponse {
        try await api.get("/api/hosts/\(name)/history?window=\(window)&metric=\(metric)", as: HistoryResponse.self)
    }
}

// metric → series (mirror hosts.js METRIC_SERIES)
struct HSeries { var key: String; var label: String; var color: Color }
enum HistoryMetric {
    static let order = ["cpu", "mem", "temp", "gpu", "net"]
    static func series(_ m: String) -> [HSeries] {
        switch m {
        case "cpu":  return [HSeries(key: "cpu.pct", label: "cpu %", color: Theme.amber)]
        case "mem":  return [HSeries(key: "mem.pct", label: "mem %", color: Theme.blue)]
        case "temp": return [HSeries(key: "cpu.temp", label: "cpu C", color: Theme.red),
                             HSeries(key: "gpu.temp_max", label: "gpu C", color: Theme.amber)]
        case "gpu":  return [HSeries(key: "gpu.util_max", label: "gpu %", color: Theme.amber),
                             HSeries(key: "gpu.temp_max", label: "gpu C", color: Theme.red)]
        case "net":  return [HSeries(key: "net.rx_bps", label: "rx", color: Theme.green),
                             HSeries(key: "net.tx_bps", label: "tx", color: Theme.amber)]
        default: return []
        }
    }
    static func isPct(_ m: String) -> Bool { m == "cpu" || m == "mem" }
}

// Argus-style segmented control
struct Segmented: View {
    var options: [String]
    @Binding var selection: String
    var body: some View {
        HStack(spacing: 0) {
            ForEach(options, id: \.self) { o in
                Button { selection = o } label: {
                    Text(o.uppercased()).font(Theme.display(10.5)).tracking(1)
                        .foregroundStyle(selection == o ? Theme.amber : Theme.inkDim)
                        .frame(maxWidth: .infinity).padding(.vertical, 7)
                        .background(selection == o ? Theme.amber.opacity(0.12) : .clear)
                }.buttonStyle(.plain)
            }
        }
        .overlay(Rectangle().stroke(Theme.line, lineWidth: 1))
    }
}

// hand-rolled time-series chart (mirror charts.js timeSeries)
struct HistoryChart: View {
    var response: HistoryResponse?
    var metric: String

    var body: some View {
        Canvas { ctx, size in draw(ctx, size) }.frame(height: 168)
    }

    private func lbl(_ s: String) -> Text { Text(s).font(Theme.mono(8)).foregroundColor(Theme.inkFaint) }

    private func draw(_ ctx: GraphicsContext, _ size: CGSize) {
        let specs = HistoryMetric.series(metric)
        let pts = response?.points ?? []
        var series: [(HSeries, [(Double, Double)])] = []
        for s in specs {
            var arr: [(Double, Double)] = []
            for p in pts { if let ts = p["ts"]?.v, let v = p[s.key]?.v { arr.append((ts, v)) } }
            if !arr.isEmpty { series.append((s, arr)) }
        }
        let padL: CGFloat = 34, padR: CGFloat = 8, padT: CGFloat = 10, padB: CGFloat = 8
        let pw = size.width - padL - padR, ph = size.height - padT - padB
        let allV = series.flatMap { $0.1.map(\.1) }
        let allT = series.flatMap { $0.1.map(\.0) }
        guard allV.count >= 2, let t0 = allT.min(), let t1 = allT.max() else {
            ctx.draw(lbl("no data yet"), at: CGPoint(x: size.width / 2, y: size.height / 2))
            return
        }
        let vMax = HistoryMetric.isPct(metric) ? 100.0 : max((allV.max() ?? 1) * 1.12, 1)
        let rows = 4
        for i in 0...rows {
            let y = padT + ph * CGFloat(i) / CGFloat(rows)
            var line = Path(); line.move(to: CGPoint(x: padL, y: y)); line.addLine(to: CGPoint(x: size.width - padR, y: y))
            ctx.stroke(line, with: .color(Theme.line), style: StrokeStyle(lineWidth: 1, dash: [3, 4]))
            let val = vMax * (1 - Double(i) / Double(rows))
            ctx.draw(lbl(val >= 100 ? String(format: "%.0f", val) : String(format: "%.1f", val)),
                     at: CGPoint(x: 2, y: y), anchor: .leading)
        }
        for (s, arr) in series {
            var path = Path(); var started = false
            for (ts, v) in arr {
                let x = padL + CGFloat((ts - t0) / max(1, t1 - t0)) * pw
                let y = padT + ph - CGFloat(min(v, vMax) / vMax) * ph
                if !started { path.move(to: CGPoint(x: x, y: y)); started = true } else { path.addLine(to: CGPoint(x: x, y: y)) }
            }
            ctx.stroke(path, with: .color(s.color), lineWidth: 1.6)
        }
        var lx = padL + 4
        for (s, _) in series {
            ctx.fill(Path(CGRect(x: lx, y: padT - 6, width: 8, height: 3)), with: .color(s.color))
            ctx.draw(lbl(s.label), at: CGPoint(x: lx + 12, y: padT - 5), anchor: .leading)
            lx += 64
        }
    }
}
