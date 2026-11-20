@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

/// A server stand-in that drops the second entry of every page, leaving a gap after the first.
private struct GappyTransport: SyncTransport {
    let server: CommitLog
    func push(_ batch: [Mutation]) async throws -> [PushResult] { [] }
    func pull(after cursor: UInt64, limit: Int) async throws -> PullResponse {
        let response = await server.pull(after: cursor, limit: limit)
        guard case let .entries(page, head) = response, page.count >= 3 else { return response }
        return .entries([page[0]] + page.dropFirst(2), head: head)
    }
}

final class PersistenceTests: XCTestCase {
    func testStateSurvivesARelaunchAndTheQueuedEditCommitsOnce() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 0)])
        try await ws.asha.sync()
        try await ws.ben.sync()

        // Ben votes offline, his push is admitted but the answer is lost, then the app dies.
        try await ws.ben.write(Names.item1, [.add(Names.votes, 1)])
        await ws.benLink.injectOnNextPush(.dropResponse)
        await ws.benLink.setOnline(true)
        _ = try? await ws.ben.sync()
        let saved = try JSONEncoder().encode(await ws.ben.persistentState())

        // Relaunch from disk.
        let state = try JSONDecoder().decode(ReplicaState.self, from: saved)
        guard let relaunched = Replica(actor: Names.ben, transport: ws.benLink, restoring: state) else {
            return XCTFail("restore refused a valid state")
        }
        let restoredOutbox = await relaunched.view().outbox.count
        XCTAssertEqual(restoredOutbox, 1)
        try await relaunched.sync()
        // A new edit after the relaunch gets a fresh key, not a reused one.
        let next = try await relaunched.write(Names.item1, [.add(Names.votes, 1)])
        XCTAssertEqual(next.clientSeq, 2)
        try await relaunched.sync()
        let votes = await ws.server.snapshot().records[Names.item1]?[Names.votes]
        XCTAssertEqual(votes, .count(2))
        let refused = await relaunched.view().rejections
        XCTAssertTrue(refused.isEmpty)
    }

    func testRestoreRefusesStateThatWouldReuseKeys() async throws {
        let ws = Workspace()
        try await ws.ben.write(Names.item1, [.add(Names.votes, 1)])
        let state = await ws.ben.persistentState()
        XCTAssertNil(Replica(actor: Names.asha, transport: ws.ashaLink, restoring: state), "another actor's state")
        let rewound = ReplicaState(actor: state.actor, confirmed: state.confirmed, nextSeq: 1,
                                   outbox: state.outbox, committedAhead: state.committedAhead)
        XCTAssertNil(Replica(actor: Names.ben, transport: ws.benLink, restoring: rewound), "nextSeq behind the outbox")
        let zero = ReplicaState(actor: state.actor, confirmed: state.confirmed, nextSeq: 0, outbox: [], committedAhead: [])
        XCTAssertNil(Replica(actor: Names.ben, transport: ws.benLink, restoring: zero), "sequence numbers start at 1")
        XCTAssertNotNil(Replica(actor: Names.ben, transport: ws.benLink, restoring: state))
    }

    func testEditQueuedAfterRestoringAStaleSaveIsReKeyedNotLost() async throws {
        let ws = Workspace()
        let stale = await ws.ben.persistentState() // saved before the edit below: the mistake
        try await ws.ben.write(Names.item1, [.set(Names.notes, .text("first"))])
        try await ws.ben.sync()
        guard let restarted = Replica(actor: Names.ben, transport: ws.benLink, restoring: stale) else {
            return XCTFail("restore")
        }
        // Queued before the first sync, with the key ben#1 that the log already holds.
        let queued = try await restarted.write(Names.item1, [.set(Names.notes, .text("second"))])
        XCTAssertEqual(queued.clientSeq, 1)
        let first = try await restarted.sync()
        XCTAssertTrue(first.rekeyed, "re-keyed, and nothing pushed until the host can save")
        let untouched = await ws.server.snapshot().records[Names.item1]?[Names.notes]
        XCTAssertEqual(untouched, .text("first"))
        try await restarted.sync()
        let rejections = await restarted.view().rejections
        XCTAssertTrue(rejections.isEmpty, "re-keyed, so not refused as an idempotency conflict")
        let notes = await ws.server.snapshot().records[Names.item1]?[Names.notes]
        XCTAssertEqual(notes, .text("second"))
    }

    func testAFreshEditSurvivesASecondRelaunchBeforeTheFirstPull() async throws {
        let ws = Workspace()
        let stale = await ws.ben.persistentState()
        try await ws.ben.write(Names.item1, [.set(Names.notes, .text("first"))])
        try await ws.ben.sync()

        // Relaunch #1 from the stale save, offline: the edit gets key ben#1 and can't sync.
        await ws.benLink.setOnline(false)
        guard let first = Replica(actor: Names.ben, transport: ws.benLink, restoring: stale) else { return XCTFail("restore") }
        let queued = try await first.write(Names.item1, [.set(Names.notes, .text("second"))])
        XCTAssertEqual(queued.clientSeq, 1)
        _ = try? await first.sync()
        let saved = try JSONDecoder().decode(ReplicaState.self, from: JSONEncoder().encode(await first.persistentState()))
        XCTAssertNotNil(saved.freshFrom, "the unverified boundary is saved")

        // Relaunch #2, online: ben#1 is still recognised as fresh and re-keyed.
        await ws.benLink.setOnline(true)
        guard let second = Replica(actor: Names.ben, transport: ws.benLink, restoring: saved) else { return XCTFail("restore") }
        let rekeying = try await second.sync()
        XCTAssertTrue(rekeying.rekeyed)
        try await second.sync()
        let rejections = await second.view().rejections
        XCTAssertTrue(rejections.isEmpty)
        let notes = await ws.server.snapshot().records[Names.item1]?[Names.notes]
        XCTAssertEqual(notes, .text("second"))
        let after = await second.persistentState()
        XCTAssertNil(after.freshFrom, "cleared once a pull reached the head")
    }

    /// The host saves after every write and every sync, as `ReplicaState` asks, and the app
    /// is killed right after each sync (before that save). The edit must still apply once.
    func testACrashAfterEachSyncNeverAppliesAReKeyedEditTwice() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 1)])
        try await ws.asha.sync()
        let stale = await ws.ben.persistentState()
        try await ws.ben.write(Names.item1, [.set(Names.notes, .text("ben#1 is taken"))])
        try await ws.ben.sync()

        func relaunch(_ state: ReplicaState) throws -> Replica {
            let decoded = try JSONDecoder().decode(ReplicaState.self, from: JSONEncoder().encode(state))
            guard let replica = Replica(actor: Names.ben, transport: ws.benLink, restoring: decoded) else {
                throw ReplicaError.sequenceExhausted // stands in for "restore refused"
            }
            return replica
        }

        let r1 = try relaunch(stale)
        try await r1.write(Names.item1, [.add(Names.votes, 10)]) // key ben#1: collides
        let s1 = await r1.persistentState()                      // saved after the write
        _ = try await r1.sync()                                   // killed before the next save
        let r2 = try relaunch(s1)
        let rekeying = try await r2.sync()
        XCTAssertTrue(rekeying.rekeyed)
        let s2 = await r2.persistentState()                      // saved after the sync
        _ = try await r2.sync()                                   // pushes; killed before saving
        let r3 = try relaunch(s2)
        _ = try await r3.sync()                                   // replays the stored outcome
        let votes = await ws.server.snapshot().records[Names.item1]?[Names.votes]
        XCTAssertEqual(votes, .count(11), "the +10 applied exactly once")
    }

    func testRestoredOutboxKeepsItsKeysWhileNewEditsAreReKeyed() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 0)])
        try await ws.asha.sync()
        // Ben's vote is admitted but the answer is lost; that state is saved.
        try await ws.ben.sync()
        try await ws.ben.write(Names.item1, [.add(Names.votes, 1)])
        await ws.benLink.injectOnNextPush(.dropResponse)
        await ws.benLink.setOnline(false)
        _ = try? await ws.ben.sync() // offline: nothing sent yet
        let saved = await ws.ben.persistentState()
        await ws.benLink.setOnline(true)
        _ = try? await ws.ben.sync() // admitted, response lost
        guard let restarted = Replica(actor: Names.ben, transport: ws.benLink, restoring: saved) else {
            return XCTFail("restore")
        }
        try await restarted.write(Names.item1, [.add(Names.votes, 1)]) // ben#2, not yet in the log
        try await restarted.sync()
        let votes = await ws.server.snapshot().records[Names.item1]?[Names.votes]
        XCTAssertEqual(votes, .count(2), "ben#1 replayed once, ben#2 applied once")
        let rejections = await restarted.view().rejections
        XCTAssertTrue(rejections.isEmpty)
    }

    func testAPullMovesTheNextKeyPastEditsTheLogAlreadyHolds() async throws {
        let ws = Workspace()
        let stale = await ws.ben.persistentState()
        try await ws.ben.write(Names.item1, [.set(Names.notes, .text("first"))])
        try await ws.ben.sync()
        guard let restarted = Replica(actor: Names.ben, transport: ws.benLink, restoring: stale) else {
            return XCTFail("restore")
        }
        try await restarted.sync() // nothing queued: only pulls, which reveals ben#1 in the log
        let next = try await restarted.write(Names.item1, [.set(Names.notes, .text("second"))])
        XCTAssertEqual(next.clientSeq, 2, "ben#1 is already taken")
        try await restarted.sync()
        let notes = await ws.server.snapshot().records[Names.item1]?[Names.notes]
        XCTAssertEqual(notes, .text("second"))
    }

    func testPendingEditsAreBounded() async throws {
        let ws = Workspace()
        await ws.ashaLink.setOnline(false)
        for _ in 0..<Replica.maxPendingEdits {
            try await ws.asha.write(Names.item1, [.add(Names.votes, 1)])
        }
        do {
            try await ws.asha.write(Names.item1, [.add(Names.votes, 1)])
            XCTFail("the outbox should be full")
        } catch {
            XCTAssertEqual(error as? ReplicaError, .outboxFull(limit: Replica.maxPendingEdits))
        }
        await ws.ashaLink.setOnline(true)
        try await ws.asha.sync()
        let afterDrain = try await ws.asha.write(Names.item1, [.add(Names.votes, 1)])
        XCTAssertEqual(afterDrain.clientSeq, UInt64(Replica.maxPendingEdits) + 1)
    }

    func testSnapshotRefusesAnEntryPastAGap() {
        var snapshot = Snapshot()
        let edit = Mutation(id: MutationID(actor: Names.asha, clientSeq: 1),
                            body: .write(record: Names.item1, ops: [.add(Names.votes, 1)], baseVersion: 0))
        XCTAssertFalse(snapshot.apply(LogEntry(seq: 2, event: .committed(edit))))
        XCTAssertEqual(snapshot.version, 0)
        XCTAssertTrue(snapshot.apply(LogEntry(seq: 1, event: .committed(edit))))
        XCTAssertFalse(snapshot.apply(LogEntry(seq: 1, event: .committed(edit))), "a replayed entry is ignored")
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.votes], .count(1))
    }

    func testReplicaNeverSkipsHistoryWhenAPageHasAGap() async throws {
        let ws = Workspace()
        for _ in 0..<3 { try await ws.asha.write(Names.item1, [.add(Names.votes, 1)]) }
        try await ws.asha.sync()
        let observer = Replica(actor: Names.ben, transport: GappyTransport(server: ws.server))
        let report = try await observer.sync()
        XCTAssertEqual(report.pulledEntries, 1, "the entry before the gap is applied")
        XCTAssertTrue(report.pullStalled, "the host is told the pull couldn't continue")
        let cursor = await observer.cursor
        XCTAssertEqual(cursor, 1, "nothing past the gap was applied")
    }
}
