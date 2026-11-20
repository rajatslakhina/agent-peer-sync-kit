@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

/// Records the states a persist handler is given.
private actor StateStore {
    private(set) var latest: ReplicaState?
    private(set) var count = 0
    func put(_ state: ReplicaState) {
        latest = state
        count += 1
    }
}

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

    func testAStaleSaveCanLoseAnEditButNeverAppliesOneTwice() async throws {
        let ws = Workspace()
        let stale = await ws.ben.persistentState() // older than what reached the server: the mistake
        try await ws.ben.write(Names.item1, [.set(Names.done, .flag(true))]) // ben#1
        try await ws.ben.sync()
        try await ws.asha.sync()
        try await ws.asha.write(Names.item1, [.set(Names.done, .flag(false))])
        try await ws.asha.sync()

        // A different edit under the reused key ben#1: refused, listed, not applied.
        guard let differs = Replica(actor: Names.ben, transport: ws.benLink, restoring: stale) else { return XCTFail("restore") }
        try await differs.write(Names.item1, [.set(Names.notes, .text("lost"))])
        let refused = try await differs.sync()
        XCTAssertEqual(refused.rejected, 1)
        let reasons = await differs.view().rejections.map(\.reason)
        XCTAssertEqual(reasons, [.idempotencyConflict])
        var record = await ws.server.snapshot().records[Names.item1]
        XCTAssertNil(record?[Names.notes])

        // An identical edit under the reused key: taken to be ben#1, reported committed with
        // the old outcome, and not applied again (Asha's later `done = false` stands).
        guard let same = Replica(actor: Names.ben, transport: ws.benLink, restoring: stale) else { return XCTFail("restore") }
        try await same.write(Names.item1, [.set(Names.done, .flag(true))])
        let replayed = try await same.sync()
        XCTAssertEqual(replayed.committed, 1)
        XCTAssertEqual(replayed.rejected, 0)
        record = await ws.server.snapshot().records[Names.item1]
        XCTAssertEqual(record?[Names.done], .flag(false))
    }

    func testPersistHandlerStoresTheStateBeforeEveryPush() async throws {
        let ws = Workspace()
        let store = StateStore()
        let ben = Replica(actor: Names.ben, transport: ws.benLink, persist: { await store.put($0) })
        let id = try await ben.write(Names.item1, [.add(Names.votes, 1)])
        try await ben.sync()
        let saved = await store.latest
        XCTAssertEqual(saved?.outbox.map(\.id), [id], "saved holding the key before the push")
        let calls = await store.count
        XCTAssertEqual(calls, 1)

        // A failing save stops the push: nothing reaches the server.
        let failing = Replica(actor: Names.asha, transport: ws.ashaLink, persist: { _ in throw TransportFailure("disk full") })
        try await failing.write(Names.item2, [.add(Names.votes, 1)])
        do { try await failing.sync(); XCTFail("persist failed, so sync must throw") } catch {}
        let item2 = await ws.server.snapshot().records[Names.item2]
        XCTAssertNil(item2)
        let queued = await failing.view().outbox.count
        XCTAssertEqual(queued, 1)
    }

    /// The host saves only now and then; the app dies right after a sync that pushed an
    /// edit the host never saved. The persist handler's write-ahead save covers it.
    func testACrashBeforeTheHostsOwnSaveIsSafeWithThePersistHandler() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.add(Names.votes, 0)])
        try await ws.asha.sync()
        let store = StateStore()
        let hostSave = await ws.ben.persistentState() // the host's last own save: before the edit
        let ben = Replica(actor: Names.ben, transport: ws.benLink, persist: { await store.put($0) })
        try await ben.write(Names.item1, [.add(Names.votes, 10)])
        _ = try await ben.sync() // pushes ben#1; the app dies before the host saves again

        let state = await store.latest ?? hostSave
        guard let relaunched = Replica(actor: Names.ben, transport: ws.benLink, restoring: state,
                                       persist: { await store.put($0) }) else { return XCTFail("restore") }
        try await relaunched.sync() // ben#1 replayed, not applied again
        let next = try await relaunched.write(Names.item1, [.add(Names.votes, 1)])
        XCTAssertEqual(next.clientSeq, 2, "the saved state knew ben#1 was taken")
        try await relaunched.sync()
        let votes = await ws.server.snapshot().records[Names.item1]?[Names.votes]
        XCTAssertEqual(votes, .count(11))
        let rejections = await relaunched.view().rejections
        XCTAssertTrue(rejections.isEmpty)
    }

    func testRestoredOutboxKeepsItsKeysAndNewEditsGetFreshOnes() async throws {
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

    func testAStateSavedBy2_0_4WithFreshFromStillDecodes() throws {
        let saved = ReplicaState(actor: Names.ben, confirmed: Snapshot(), nextSeq: 3, outbox: [], committedAhead: [])
        guard case var .object(fields)? = JSONValue.parse(String(decoding: try JSONEncoder().encode(saved), as: UTF8.self)) else {
            return XCTFail("a state encodes as a JSON object")
        }
        fields["freshFrom"] = .number(2) // the field 2.0.3 and 2.0.4 wrote
        let decoded = try JSONDecoder().decode(ReplicaState.self, from: Data(JSONValue.object(fields).jsonString().utf8))
        XCTAssertEqual(decoded, saved)
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
