import Foundation

/// How a replica reaches the commit log. The transport carries the caller's credential, so
/// the identity a push is admitted under is the transport's, not something the replica picks.
public protocol SyncTransport: Sendable {
    func push(_ batch: [Mutation]) async throws -> [PushResult]
    func pull(after cursor: UInt64, limit: Int) async throws -> PullResponse
}

public enum ReplicaError: Error, Hashable, Sendable {
    /// The replica's 64-bit client sequence is used up. Not reachable in practice; reported
    /// instead of wrapping, because a wrapped sequence would collide with old idempotency keys.
    case sequenceExhausted
    /// Too many edits are waiting (unanswered, or answered but not yet pulled). Writes are
    /// refused until a sync drains some, so a long offline stretch can't grow memory forever.
    case outboxFull(limit: Int)
}

/// Everything a replica needs to survive a relaunch: confirmed state, outbox, next key.
/// Pass it back to `Replica(actor:transport:restoring:)`. The library does no I/O itself.
///
/// **The key rule.** An edit gets its idempotency key when it is written and keeps it for
/// its whole life; the server applies a key at most once. So no edit is ever applied twice,
/// whatever crashes or retries happen. To make sure no key reaches the network unsaved,
/// give the replica a `persist` handler: it is called with this state, and must have stored
/// it durably, before every push (a push doesn't happen if it throws).
///
/// **What a stale save can still cost.** Restoring a state older than the last one persisted
/// (a lost write, a restore from backup) can hand out keys the server already holds. Edits
/// under such a key are never applied twice, but they can be lost: a different body is
/// refused as `.idempotencyConflict` (listed in `rejections`); an identical body is taken to
/// be the old edit and reported as committed with the old outcome, without being applied
/// again. After its first pull a restored replica moves its next key past every key the log
/// shows, which narrows this to keys whose only outcome was a refusal.
public struct ReplicaState: Hashable, Sendable, Codable {
    public let actor: ActorID
    public let confirmed: Snapshot
    public let nextSeq: UInt64
    public let outbox: [Mutation]
    public let committedAhead: [CommittedEdit]
}

/// An edit the server committed at `seq` that the replica hasn't pulled yet.
public struct CommittedEdit: Hashable, Sendable, Codable {
    public let mutation: Mutation
    public let seq: UInt64
}

/// An edit the server refused, kept so the UI can say why.
public struct RejectedEdit: Hashable, Sendable, Identifiable {
    public let mutation: Mutation
    public let reason: RejectionReason
    public var id: MutationID { mutation.id }
}

public struct SyncReport: Hashable, Sendable {
    public var committed = 0
    public var proposed = 0
    public var rejected = 0
    public var pulledEntries = 0
    public var resetFromSnapshot = false
    /// The server reported a sequence gap at the head of the outbox. The outbox is kept;
    /// this means client and server disagree about history and needs an operator.
    public var stalled = false
    /// The pull stopped short of the server's head: a page didn't continue from the cursor
    /// (a gap in history; nothing past it was applied), a page made no progress while the
    /// server reported more, or the page budget ran out. A server that keeps doing this is
    /// broken, and the host should surface it rather than keep calling `sync()` silently.
    public var pullStalled = false
    public init() {}
}

/// What a UI renders: the confirmed state, the state with this replica's unanswered and
/// not-yet-pulled edits laid on top, and what is still in flight.
public struct ReplicaView: Hashable, Sendable {
    public let actor: ActorID
    /// Shared state as of `confirmed.version`, exactly as the server admitted it.
    public let confirmed: Snapshot
    /// `confirmed` plus this replica's own edits that the server has not answered yet, or
    /// has committed beyond the cursor. Rebuilt after every pull (rebase-on-pull).
    public let visible: Snapshot
    public let outbox: [Mutation]
    public let rejections: [RejectedEdit]
    /// Records with an edit in the overlay, for "pending" badges.
    public let recordsWithLocalEdits: Set<RecordID>
}

/// A local replica of the shared state, used the same way by a person's phone and by an agent.
///
/// Edits go to an outbox with a fresh idempotency key and show up immediately in `visible`.
/// `sync()` pushes the outbox, then pulls the log from the replica's cursor and rebuilds
/// `visible` on top of the new confirmed state.
public actor Replica {
    /// Stores a `ReplicaState` durably. Called before every push; throwing cancels the push.
    public typealias PersistHandler = @Sendable (ReplicaState) async throws -> Void

    public static let maxRetainedRejections = 50
    public static let maxPendingEdits = 10_000

    public nonisolated let actor: ActorID
    private let transport: any SyncTransport
    private let pushBatchSize: Int
    private let pullPageSize: Int
    private let persist: PersistHandler?

    private var confirmed = Snapshot()
    private var nextSeq: UInt64 = 1
    private var outbox: [Mutation] = []
    /// Committed by the server at a sequence this replica hasn't pulled yet. Kept in the
    /// overlay until the cursor passes it, so an acknowledged edit doesn't flicker out.
    private var committedAhead: [CommittedEdit] = []
    private var rejections: [RejectedEdit] = []
    private var inFlight: Task<SyncReport, any Error>?
    /// How many `sync()` calls joined a sync that was already running. Observed by tests.
    private(set) var joinedSyncs = 0
    /// Pages one sync may pull before reporting `pullStalled`. Internal so tests can lower it.
    var pullPageBudget = 10_000
    func setPullPageBudget(_ pages: Int) { pullPageBudget = max(1, pages) }

    /// Batch and page sizes are clamped to `1...500`. `persist`, if given, is called with
    /// `persistentState()` before every push (see `ReplicaState`).
    public init(actor: ActorID, transport: any SyncTransport, pushBatchSize: Int = 50, pullPageSize: Int = 200,
                persist: PersistHandler? = nil) {
        self.actor = actor
        self.transport = transport
        self.pushBatchSize = min(max(pushBatchSize, 1), 500)
        self.pullPageSize = min(max(pullPageSize, 1), 500)
        self.persist = persist
    }

    /// Restores a replica saved with `persistentState()`. Returns nil if the state belongs
    /// to a different actor, or is internally inconsistent (an outbox entry from another
    /// actor, or a sequence number at or below one already used).
    public init?(actor: ActorID, transport: any SyncTransport, restoring state: ReplicaState,
                 pushBatchSize: Int = 50, pullPageSize: Int = 200, persist: PersistHandler? = nil) {
        let used = state.outbox.map(\.id) + state.committedAhead.map(\.mutation.id)
        guard state.actor == actor, state.nextSeq >= 1,
              used.allSatisfy({ $0.actor == actor && $0.clientSeq < state.nextSeq }) else {
            return nil
        }
        self.actor = actor
        self.transport = transport
        self.pushBatchSize = min(max(pushBatchSize, 1), 500)
        self.pullPageSize = min(max(pullPageSize, 1), 500)
        self.persist = persist
        confirmed = state.confirmed
        nextSeq = state.nextSeq
        outbox = state.outbox
        committedAhead = state.committedAhead
        // Same rule as `bumpSequencePastAdmittedEdits()`, inlined: init is nonisolated.
        var next = state.nextSeq
        let admitted = state.confirmed.appliedThrough[actor] ?? 0
        if admitted < UInt64.max, next <= admitted { next = admitted + 1 }
        nextSeq = next
    }

    public func persistentState() -> ReplicaState {
        ReplicaState(actor: actor, confirmed: confirmed, nextSeq: nextSeq, outbox: outbox,
                     committedAhead: committedAhead)
    }

    // MARK: Local edits

    @discardableResult
    public func write(_ record: RecordID, _ ops: [FieldOp]) throws -> MutationID {
        try enqueue(.write(record: record, ops: ops, baseVersion: confirmed.version))
    }

    @discardableResult
    public func review(_ proposal: UInt64, _ decision: ReviewDecision) throws -> MutationID {
        try enqueue(.review(proposal: proposal, decision: decision))
    }

    private func enqueue(_ body: MutationBody) throws -> MutationID {
        guard nextSeq < UInt64.max else { throw ReplicaError.sequenceExhausted }
        guard outbox.count + committedAhead.count < Self.maxPendingEdits else {
            throw ReplicaError.outboxFull(limit: Self.maxPendingEdits)
        }
        let id = MutationID(actor: actor, clientSeq: nextSeq)
        nextSeq += 1
        outbox.append(Mutation(id: id, body: body))
        return id
    }

    // MARK: Sync

    /// Push, then pull to the server's head. Concurrent callers share one sync: a second call
    /// while one is running awaits the same work instead of pushing the same outbox twice.
    ///
    /// A caller that joins a running sync and still has an edit of its own in the outbox when
    /// that sync ends (it was written after the running push had passed) gets one more sync,
    /// so `write(...); try await sync()` always attempts to send that write.
    @discardableResult
    public func sync() async throws -> SyncReport {
        guard let running = inFlight else { return try await startOrJoin() }
        joinedSyncs += 1
        let newestAtCall = nextSeq - 1 // nextSeq starts at 1, only grows, and restore requires >= 1
        let report = try await running.value
        guard outbox.contains(where: { $0.id.clientSeq <= newestAtCall }) else { return report }
        return try await startOrJoin()
    }

    /// Starts a sync, or joins one that is running. No follow-up round, so it can't loop.
    private func startOrJoin() async throws -> SyncReport {
        if let running = inFlight { return try await running.value }
        let task = Task { try await self.performSync() }
        inFlight = task
        return try await task.value
    }

    private func performSync() async throws -> SyncReport {
        // Cleared here, on the actor, the moment the work ends, so a sync requested after
        // that point starts fresh instead of receiving this (already finished) report.
        // The task can't start before `sync()` stores it: `sync()` holds the actor until
        // it suspends on `task.value`.
        defer { inFlight = nil }
        var report = SyncReport()
        // A failed push doesn't stop the pull: with writes refused but reads working (a
        // partial outage), the replica still rebases its pending edits onto fresh state.
        // The push error is still thrown, after the pull, so the caller knows to retry.
        var pushFailure: (any Error)?
        do {
            try await pushOutbox(into: &report)
        } catch {
            pushFailure = error
        }
        try await pullToHead(into: &report)
        if let pushFailure { throw pushFailure }
        return report
    }

    private func pushOutbox(into report: inout SyncReport) async throws {
        // Bounded: each round either removes at least one mutation from the outbox or stops.
        while !outbox.isEmpty {
            let batch = Array(outbox.prefix(pushBatchSize))
            // Write-ahead: the state holding these keys is stored before they leave the device.
            if let persist { try await persist(persistentState()) }
            let results = try await transport.push(batch)
            // Edits made while the push was in flight were appended behind the batch, and
            // results are matched by idempotency key, so they can't be mistaken for answered ones.
            let before = outbox.count
            for result in results { settle(result, report: &report) }
            if outbox.count == before { return }
        }
    }

    private func settle(_ result: PushResult, report: inout SyncReport) {
        guard let index = outbox.firstIndex(where: { $0.id == result.id }), outbox.indices.contains(index) else {
            return // an answer for something already settled (e.g. a duplicated response)
        }
        let mutation = outbox[index]
        switch result.outcome {
        case let .committed(seq):
            outbox.remove(at: index)
            report.committed += 1
            if seq > confirmed.version { committedAhead.append(CommittedEdit(mutation: mutation, seq: seq)) }
        case .proposed:
            outbox.remove(at: index)
            report.proposed += 1
        case .rejected(.batchTooLarge):
            break // not looked at by the server: stays queued for the next round
        case let .rejected(reason):
            outbox.remove(at: index)
            report.rejected += 1
            rejections.append(RejectedEdit(mutation: mutation, reason: reason))
            if rejections.count > Self.maxRetainedRejections {
                rejections.removeFirst(rejections.count - Self.maxRetainedRejections)
            }
        case .outOfOrder:
            report.stalled = true
        }
    }

    private func pullToHead(into report: inout SyncReport) async throws {
        // Bounded so a misbehaving server can't keep a sync running forever.
        for _ in 0..<pullPageBudget {
            let cursor = confirmed.version
            let response = try await transport.pull(after: cursor, limit: pullPageSize)
            switch response {
            case let .snapshot(snapshot):
                confirmed = snapshot
                report.resetFromSnapshot = true
                dropPulledCommits()
                return
            case let .entries(page, head):
                // Apply in order and stop at the first entry that isn't the next one: a replica
                // never skips history. The next page is requested from wherever it stopped.
                var applied = 0
                for entry in page where entry.seq > confirmed.version {
                    guard confirmed.apply(entry) else {
                        report.pullStalled = true
                        break
                    }
                    applied += 1
                }
                report.pulledEntries += applied
                dropPulledCommits()
                if report.pullStalled { return }
                if confirmed.version >= head { return }
                // A page that made no progress while the server says there is more: report
                // it rather than claim to be caught up.
                if confirmed.version == cursor {
                    report.pullStalled = true
                    return
                }
            }
        }
        report.pullStalled = true // page budget exhausted before reaching the head
    }

    private func dropPulledCommits() {
        committedAhead.removeAll { $0.seq <= confirmed.version }
        bumpSequencePastAdmittedEdits()
    }

    /// If the log already holds this actor's mutations up to some sequence number (for
    /// instance after a relaunch from an older save), never hand out those numbers again.
    /// (Edits already queued keep their keys; see `ReplicaState`.)
    private func bumpSequencePastAdmittedEdits() {
        let admitted = confirmed.appliedThrough[actor] ?? 0
        if admitted < UInt64.max, nextSeq <= admitted { nextSeq = admitted + 1 }
    }

    // MARK: Reading

    public func view() -> ReplicaView {
        var records = confirmed.records
        var touched = Set<RecordID>()
        // Overlay order: committed-ahead edits in commit order, then unanswered edits in
        // the order they were made. An unanswered edit the confirmed log already contains
        // (its response was lost, but a pull brought it in) is skipped, so it isn't counted
        // twice; it stays in the outbox until a push learns its outcome. Overlay writes are
        // stamped with the pseudo-version `UInt64.max`; read only field values from `visible`.
        let admitted = confirmed.appliedThrough[actor] ?? 0
        let unanswered = outbox.filter { $0.id.clientSeq > admitted }
        let overlay = committedAhead.sorted { $0.seq < $1.seq }.map(\.mutation) + unanswered
        for mutation in overlay {
            guard case let .write(record, ops, _) = mutation.body else { continue }
            Snapshot.applyOps(ops, to: record, by: actor, at: .max, in: &records)
            touched.insert(record)
        }
        var visible = confirmed
        visible.records = records
        return ReplicaView(actor: actor, confirmed: confirmed, visible: visible, outbox: outbox,
                           rejections: rejections, recordsWithLocalEdits: touched)
    }

    public var cursor: UInt64 { confirmed.version }
}
