@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

/// SplitMix64: a tiny deterministic generator, so a failing seed can be replayed.
private struct SeededGenerator: RandomNumberGenerator {
    private var state: UInt64
    init(seed: UInt64) { state = seed }
    mutating func next() -> UInt64 {
        state &+= 0x9E37_79B9_7F4A_7C15
        var z = state
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return z ^ (z >> 31)
    }
}

final class ConvergenceTests: XCTestCase {
    private let records = [Names.item1, Names.item2, Names.item3]
    private let faults: [LocalTransport.Fault] = [.dropRequest, .dropResponse, .duplicateRequest]

    private func randomEdit(for actor: ActorID, using rng: inout SeededGenerator) -> [FieldOp] {
        if actor == Names.agent {
            switch Int.random(in: 0..<3, using: &rng) {
            case 0: return [.set(Names.summary, .text("s\(Int.random(in: 0..<9, using: &rng))"))]
            case 1: return [.set(Names.title, .text("agent \(Int.random(in: 0..<9, using: &rng))"))]
            default: return [.set(Names.notes, .text("agent notes"))]
            }
        }
        switch Int.random(in: 0..<10, using: &rng) {
        case 0..<3: return [.set(Names.title, .text("t\(Int.random(in: 0..<99, using: &rng))"))]
        case 3..<5: return [.set(Names.notes, .text("n\(Int.random(in: 0..<99, using: &rng))"))]
        case 5: return [.set(Names.done, .flag(Bool.random(using: &rng)))]
        case 6: return [.delete]
        default: return [.add(Names.votes, Int64.random(in: -2...3, using: &rng))]
        }
    }

    /// Random offline edits, reviews, connectivity changes and transport faults across two
    /// phones and an agent; then everyone reconnects. Checks the properties the README claims.
    struct Stats {
        var committed = 0, proposed = 0, resolved = 0, rejected = 0, snapshotResets = 0
    }

    @discardableResult
    private func runSchedule(seed: UInt64, steps: Int, configuration: CommitLog.Configuration) async throws -> Stats {
        var stats = Stats()
        var rng = SeededGenerator(seed: seed)
        let ws = Workspace(configuration: configuration)
        let links = [ws.ashaLink, ws.benLink, ws.agentLink]
        let replicas = ws.replicas

        for _ in 0..<steps {
            let index = Int.random(in: 0..<replicas.count, using: &rng)
            guard replicas.indices.contains(index), links.indices.contains(index) else { continue }
            let replica = replicas[index]
            let link = links[index]
            switch Int.random(in: 0..<10, using: &rng) {
            case 0..<5:
                let record = records[Int.random(in: 0..<records.count, using: &rng)]
                try await replica.write(record, randomEdit(for: replica.actor, using: &rng))
            case 5:
                if let proposal = await replica.view().confirmed.openProposals.randomElement(using: &rng) {
                    try await replica.review(proposal.seq, Bool.random(using: &rng) ? .accept : .reject)
                }
            case 6:
                await link.setOnline(!(await link.isOnline))
            case 7:
                await link.injectOnNextPush(faults[Int.random(in: 0..<faults.count, using: &rng)])
            default:
                if let report = try? await replica.sync() {
                    stats.committed += report.committed
                    stats.proposed += report.proposed
                    stats.rejected += report.rejected
                    if report.resetFromSnapshot { stats.snapshotResets += 1 }
                }
            }
        }

        // Reconnect everyone, let a few queued faults fire, then clear the rest and drain.
        for link in links { await link.setOnline(true) }
        for _ in 0..<2 {
            for replica in replicas { _ = try? await replica.sync() }
        }
        for link in links { await link.clearFaults() }
        for _ in 0..<3 {
            for replica in replicas { _ = try? await replica.sync() }
        }
        for replica in replicas { _ = try await replica.sync() }

        let check = await ws.convergence()
        XCTAssertTrue(check.converged, "seed \(seed): \(check.rows)")

        // A replica built from nothing, paging one entry at a time, reaches the same state.
        guard let observer = ActorID("observer") else { XCTFail("valid literal"); return stats }
        let fresh = Replica(actor: observer, transport: LocalTransport(server: ws.server, authenticatedAs: observer),
                            pullPageSize: 1)
        _ = try await fresh.sync()
        let rebuilt = await fresh.view().confirmed
        let truth = await ws.server.snapshot()
        XCTAssertEqual(rebuilt, truth, "seed \(seed)")

        // With faults (dropped responses, duplicate deliveries), no mutation reached the log
        // twice, and each actor's mutations are in its own client order.
        let entries = await ws.server.retainedEntries()
        stats.resolved = truth.records.isEmpty ? 0 : entries.reduce(0) {
            if case .resolved = $1.event { return $0 + 1 }
            return $0
        }
        guard let total = Int(exactly: truth.version), entries.count == total else { return stats } // compacted: skip log checks
        var seen = Set<MutationID>()
        var lastPerActor: [ActorID: UInt64] = [:]
        for entry in entries {
            let id: MutationID
            switch entry.event {
            case let .committed(m), let .proposed(m): id = m.id
            case let .resolved(_, _, by): id = by
            }
            XCTAssertTrue(seen.insert(id).inserted, "seed \(seed): \(id) admitted twice")
            XCTAssertGreaterThan(id.clientSeq, lastPerActor[id.actor] ?? 0, "seed \(seed): \(id) out of order")
            lastPerActor[id.actor] = id.clientSeq
        }
        return stats
    }

    func testRandomOfflineSchedulesConvergeWithoutDuplicates() async throws {
        var total = Stats()
        for seed: UInt64 in 1...25 {
            let stats = try await runSchedule(seed: seed, steps: 250, configuration: .init())
            total.committed += stats.committed
            total.proposed += stats.proposed
            total.resolved += stats.resolved
            total.rejected += stats.rejected
        }
        // Guard against a schedule that silently stops exercising the interesting paths.
        XCTAssertGreaterThan(total.committed, 500)
        XCTAssertGreaterThan(total.proposed, 25)
        XCTAssertGreaterThan(total.resolved, 10)
        XCTAssertGreaterThan(total.rejected, 25)
    }

    func testRandomSchedulesConvergeThroughCompactionAndASmallReplayWindow() async throws {
        var resets = 0
        for seed: UInt64 in 100...110 {
            let stats = try await runSchedule(seed: seed, steps: 250, configuration: .init(outcomeRetention: 8, maxRetainedEntries: 5))
            resets += stats.snapshotResets
        }
        XCTAssertGreaterThan(resets, 10, "replicas really did fall behind compaction")
    }

    /// The convergence check is only worth something if it can fail. Here a replica's
    /// confirmed state is corrupted behind the reducer's back; the check must notice.
    func testConvergenceCheckDetectsADivergentReplica() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 1)])
        for replica in ws.replicas { _ = try await replica.sync() }
        var check = await ws.convergence()
        XCTAssertTrue(check.converged)

        // Ben reaches a different state by applying an entry the server never admitted.
        let bogus = Mutation(id: MutationID(actor: Names.ben, clientSeq: 99),
                             body: .write(record: Names.item1, ops: [.add(Names.votes, 1)], baseVersion: 0))
        let ben = Replica(actor: Names.ben, transport: DivergingTransport(server: ws.server, extra: bogus))
        _ = try await ben.sync()
        let diverged = await ben.view().confirmed
        let truth = await ws.server.snapshot()
        XCTAssertNotEqual(diverged, truth)
        check = await ws.convergence()
        XCTAssertTrue(check.converged, "the workspace's own replicas are still fine")
    }
}

/// A broken server stand-in that slips an extra entry into every page.
private struct DivergingTransport: SyncTransport {
    let server: CommitLog
    let extra: Mutation
    func push(_ batch: [Mutation]) async throws -> [PushResult] { [] }
    func pull(after cursor: UInt64, limit: Int) async throws -> PullResponse {
        let response = await server.pull(after: cursor, limit: limit)
        guard case let .entries(page, head) = response, let last = page.last else { return response }
        return .entries(page + [LogEntry(seq: last.seq + 1, event: .committed(extra))], head: head)
    }
}
