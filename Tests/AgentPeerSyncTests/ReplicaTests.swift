@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

/// Forwards to a commit log, but can hold pushes at a gate and fail pulls on demand.
private actor ControllableTransport: SyncTransport {
    let server: CommitLog
    let identity: ActorID
    var failPulls = false
    var failPushes = false
    var gatePulls = false
    private var pullWaiting: [CheckedContinuation<Void, Never>] = []
    var parkedPulls: Int { pullWaiting.count }
    func setGatePulls(_ value: Bool) {
        gatePulls = value
        if !value {
            let resumed = pullWaiting
            pullWaiting.removeAll()
            for continuation in resumed { continuation.resume() }
        }
    }
    var emptyPagesEnabled = false
    func setEmptyPages(_ value: Bool) { emptyPagesEnabled = value }
    private(set) var pushCount = 0
    private var gated = false
    private var waiting: [CheckedContinuation<Void, Never>] = []

    init(server: CommitLog, identity: ActorID) {
        self.server = server
        self.identity = identity
    }

    func setFailPulls(_ value: Bool) { failPulls = value }
    func setFailPushes(_ value: Bool) { failPushes = value }
    func closeGate() { gated = true }
    func openGate() {
        gated = false
        let resumed = waiting
        waiting.removeAll()
        for continuation in resumed { continuation.resume() }
    }
    var parkedPushes: Int { waiting.count }

    func push(_ batch: [Mutation]) async throws -> [PushResult] {
        pushCount += 1
        if failPushes { throw TransportFailure("push refused") }
        if gated { await withCheckedContinuation { waiting.append($0) } }
        return await server.push(batch, as: identity)
    }

    func pull(after cursor: UInt64, limit: Int) async throws -> PullResponse {
        if failPulls { throw TransportFailure("pull failed") }
        if gatePulls { await withCheckedContinuation { pullWaiting.append($0) } }
        if emptyPagesEnabled { return .entries([], head: await server.head) }
        return await server.pull(after: cursor, limit: limit)
    }
}

final class ReplicaTests: XCTestCase {
    private func makeServer(_ configuration: CommitLog.Configuration = .init()) -> CommitLog {
        CommitLog(schema: SampleWorkspace.schema, policies: SampleWorkspace.policies, configuration: configuration)
    }

    func testLocalEditIsVisibleBeforeItSyncs() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.set(Names.title, .text("Offline title"))])
        let view = await ws.asha.view()
        XCTAssertEqual(view.visible.records[Names.item1]?[Names.title], .text("Offline title"))
        XCTAssertNil(view.confirmed.records[Names.item1])
        XCTAssertEqual(view.outbox.count, 1)
        XCTAssertEqual(view.recordsWithLocalEdits, [Names.item1])
    }

    func testCommittedEditStaysVisibleWhenThePullFails() async throws {
        let server = makeServer()
        let link = ControllableTransport(server: server, identity: Names.asha)
        let replica = Replica(actor: Names.asha, transport: link)
        try await replica.write(Names.item1, [.set(Names.title, .text("Mine"))])
        await link.setFailPulls(true)
        do {
            try await replica.sync()
            XCTFail("pull should fail")
        } catch {}
        let view = await replica.view()
        XCTAssertTrue(view.outbox.isEmpty, "the push was answered")
        XCTAssertEqual(view.confirmed.version, 0, "nothing pulled yet")
        XCTAssertEqual(view.visible.records[Names.item1]?[Names.title], .text("Mine"), "no flicker")
    }

    func testPulledCommitsLeaveTheOverlaySoNewerRemoteValuesShow() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.set(Names.notes, .text("Asha's notes"))])
        try await ws.asha.sync()
        try await ws.ben.sync()
        try await ws.ben.write(Names.item1, [.set(Names.notes, .text("Ben's notes"))])
        try await ws.ben.sync()
        try await ws.asha.sync()
        let view = await ws.asha.view()
        XCTAssertEqual(view.visible.records[Names.item1]?[Names.notes], .text("Ben's notes"))
        XCTAssertTrue(view.recordsWithLocalEdits.isEmpty)
    }

    func testPendingEditIsRebasedOnTopOfPulledChangesWhenOnlyPushesFail() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.set(Names.title, .text("T")), .add(Names.votes, 0)])
        try await ws.asha.sync()
        let link = ControllableTransport(server: ws.server, identity: Names.ben)
        let ben = Replica(actor: Names.ben, transport: link)
        try await ben.sync()

        await link.setFailPushes(true)
        try await ben.write(Names.item1, [.add(Names.votes, 1)])
        try await ws.asha.write(Names.item1, [.add(Names.votes, 5), .set(Names.notes, .text("from Asha"))])
        try await ws.asha.sync()
        do {
            try await ben.sync()
            XCTFail("push should fail")
        } catch {}

        // Ben's unsent vote is replayed on top of Asha's freshly pulled change.
        let view = await ben.view()
        XCTAssertEqual(view.confirmed.version, 2, "the pull ran even though the push failed")
        XCTAssertEqual(view.confirmed.records[Names.item1]?[Names.votes], .count(5))
        XCTAssertEqual(view.visible.records[Names.item1]?[Names.votes], .count(6))
        XCTAssertEqual(view.visible.records[Names.item1]?[Names.notes], .text("from Asha"))
        XCTAssertEqual(view.outbox.count, 1)
    }

    func testLostResponseIsRetriedWithoutDoubleCounting() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 0)])
        try await ws.asha.sync()
        try await ws.ben.sync()
        try await ws.ben.write(Names.item1, [.add(Names.votes, 1)])
        await ws.benLink.injectOnNextPush(.dropResponse)
        do {
            try await ws.ben.sync()
            XCTFail("response was dropped")
        } catch {}
        let afterLoss = await ws.ben.view()
        XCTAssertEqual(afterLoss.outbox.count, 1, "unanswered edits stay queued")
        // The pull after the failed push brought the committed vote in; the overlay must not
        // add the still-queued copy on top of it.
        XCTAssertEqual(afterLoss.confirmed.records[Names.item1]?[Names.votes], .count(1))
        XCTAssertEqual(afterLoss.visible.records[Names.item1]?[Names.votes], .count(1))
        try await ws.ben.sync()
        let snapshot = await ws.server.snapshot()
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.votes], .count(1))
        let delivered = await ws.benLink.pushRequestsDelivered
        XCTAssertEqual(delivered, 2, "the server really did see the batch twice")
    }

    func testDuplicatedDeliveryAppliesOnce() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 2)])
        await ws.ashaLink.injectOnNextPush(.duplicateRequest)
        let report = try await ws.asha.sync()
        XCTAssertEqual(report.committed, 1)
        let snapshot = await ws.server.snapshot()
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.votes], .count(2))
    }

    func testRefusedEditLeavesTheOutboxAndIsReported() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.set(Names.title, .text("T"))])
        try await ws.asha.sync()
        try await ws.agent.sync()
        try await ws.agent.write(Names.item1, [.set(Names.done, .flag(true))])
        let report = try await ws.agent.sync()
        XCTAssertEqual(report.rejected, 1)
        let view = await ws.agent.view()
        XCTAssertTrue(view.outbox.isEmpty)
        XCTAssertEqual(view.rejections.map(\.reason), [.notPermitted(Names.done)])
        XCTAssertNil(view.visible.records[Names.item1]?[Names.done], "the refused value is gone from the overlay")
    }

    func testRejectionHistoryIsBounded() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.set(Names.title, .text("T"))])
        try await ws.asha.sync()
        try await ws.agent.sync()
        for _ in 0..<(Replica.maxRetainedRejections + 7) {
            try await ws.agent.write(Names.item1, [.add(Names.votes, 1)])
        }
        try await ws.agent.sync()
        let count = await ws.agent.view().rejections.count
        XCTAssertEqual(count, Replica.maxRetainedRejections)
    }

    func testReplicaBehindCompactionResetsFromTheSnapshot() async throws {
        let ws = Workspace(configuration: .init(maxRetainedEntries: 2))
        for value in 0..<5 {
            try await ws.asha.write(Names.item1, [.set(Names.notes, .text("v\(value)"))])
            try await ws.asha.sync()
        }
        let report = try await ws.ben.sync()
        XCTAssertTrue(report.resetFromSnapshot)
        let convergence = await ws.convergence()
        XCTAssertTrue(convergence.rows.first { $0.actor == Names.ben }?.matchesServer ?? false)
    }

    func testConcurrentSyncCallsShareOnePush() async throws {
        let server = makeServer()
        let link = ControllableTransport(server: server, identity: Names.asha)
        let replica = Replica(actor: Names.asha, transport: link)
        try await replica.write(Names.item1, [.add(Names.votes, 1)])
        await link.closeGate()
        let first = Task { try await replica.sync() }
        // Wait until the first sync's push is parked at the gate.
        var spins = 0
        while await link.parkedPushes == 0, spins < 10_000 {
            await Task.yield()
            spins += 1
        }
        let parked = await link.parkedPushes
        XCTAssertEqual(parked, 1)
        let second = Task { try await replica.sync() }
        // Deterministic: wait until the second call has actually joined the running sync.
        spins = 0
        while await replica.joinedSyncs == 0, spins < 10_000 {
            await Task.yield()
            spins += 1
        }
        let joined = await replica.joinedSyncs
        XCTAssertEqual(joined, 1)
        await link.openGate()
        let a = try await first.value
        let b = try await second.value
        XCTAssertEqual(a, b)
        let pushes = await link.pushCount
        XCTAssertEqual(pushes, 1, "the second caller joined the running sync")
    }

    func testEditsCutFromAnOversizedBatchStayQueuedAndCommitLater() async throws {
        let server = makeServer(.init(maxPushBatch: 2))
        let replica = Replica(actor: Names.asha, transport: LocalTransport(server: server, authenticatedAs: Names.asha),
                              pushBatchSize: 5)
        for _ in 0..<5 { try await replica.write(Names.item1, [.add(Names.votes, 1)]) }
        let report = try await replica.sync()
        XCTAssertEqual(report.committed, 5)
        XCTAssertEqual(report.rejected, 0)
        let votes = await server.snapshot().records[Names.item1]?[Names.votes]
        XCTAssertEqual(votes, .count(5))
    }

    func testAJoinerWhoseEditMissedTheRunningPushGetsAnotherRound() async throws {
        let server = makeServer()
        let link = ControllableTransport(server: server, identity: Names.asha)
        let replica = Replica(actor: Names.asha, transport: link)
        try await replica.write(Names.item1, [.add(Names.votes, 1)])
        await link.setGatePulls(true)
        let first = Task { try await replica.sync() }
        var spins = 0
        while await link.parkedPulls == 0, spins < 10_000 {
            await Task.yield()
            spins += 1
        }
        // The running sync has pushed and is now pulling. A new edit, then a joining sync.
        try await replica.write(Names.item1, [.add(Names.votes, 10)])
        let second = Task { try await replica.sync() }
        spins = 0
        while await replica.joinedSyncs == 0, spins < 10_000 {
            await Task.yield()
            spins += 1
        }
        await link.setGatePulls(false)
        _ = try await first.value
        let report = try await second.value
        XCTAssertEqual(report.committed, 1, "the joiner's own edit went out in a follow-up round")
        let votes = await server.snapshot().records[Names.item1]?[Names.votes]
        XCTAssertEqual(votes, .count(11))
    }

    func testAPageThatMakesNoProgressIsReportedAsStalled() async throws {
        let server = makeServer()
        let writer = Replica(actor: Names.asha, transport: LocalTransport(server: server, authenticatedAs: Names.asha))
        try await writer.write(Names.item1, [.add(Names.votes, 1)])
        try await writer.sync()
        let link = ControllableTransport(server: server, identity: Names.ben)
        await link.setEmptyPages(true)
        let reader = Replica(actor: Names.ben, transport: link)
        let report = try await reader.sync()
        XCTAssertTrue(report.pullStalled, "head is 1 but the page was empty")
        let cursor = await reader.cursor
        XCTAssertEqual(cursor, 0)
    }

    func testASyncAfterAFinishedSyncStartsFresh() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 1)])
        _ = try await ws.asha.sync()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 1)])
        let second = try await ws.asha.sync()
        XCTAssertEqual(second.committed, 1)
        let snapshot = await ws.server.snapshot()
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.votes], .count(2))
    }

    func testOfflineReviewDecisionSyncsLikeAnEdit() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.set(Names.title, .text("T"))])
        try await ws.asha.sync()
        try await ws.agent.sync()
        try await ws.agent.write(Names.item1, [.set(Names.title, .text("Agent T"))])
        try await ws.agent.sync()
        try await ws.ben.sync()
        let proposal = await ws.ben.view().confirmed.openProposals.first
        XCTAssertEqual(proposal?.author, Names.agent)
        guard let proposal else { return }

        await ws.benLink.setOnline(false)
        try await ws.ben.review(proposal.seq, .accept)
        do { try await ws.ben.sync(); XCTFail("offline") } catch {}
        await ws.benLink.setOnline(true)
        try await ws.ben.sync()
        let title = await ws.ben.view().confirmed.records[Names.item1]?[Names.title]
        XCTAssertEqual(title, .text("Agent T"))
    }
}
