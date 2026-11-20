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

/// Everything a replica needs to survive a relaunch. Persist it after every `write`,
/// `review` and `sync` (e.g. as JSON in a file or a SQLite row) and pass it back to
/// `Replica(actor:transport:restoring:)`. The library does no I/O itself.
///
/// It must be saved *before* the edit it covers can reach the network: a replica that
/// restarts from an older state would reuse idempotency keys for different edits, which
/// the server refuses as `.idempotencyConflict` rather than applying the wrong thing.
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
    /// A pulled page didn't continue from the cursor (a gap in history). Nothing past the
    /// gap was applied and the cursor stayed where it was; a server that keeps doing this is
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
    public static let maxRetainedRejections = 50
    public static let maxPendingEdits = 10_000

    public nonisolated let actor: ActorID
    private let transport: any SyncTransport
    private let pushBatchSize: Int
    private let pullPageSize: Int

    private var confirmed = Snapshot()
    private var nextSeq: UInt64 = 1
    private var outbox: [Mutation] = []
    /// Committed by the server at a sequence this replica hasn't pulled yet. Kept in the
    /// overlay until the cursor passes it, so an acknowledged edit doesn't flicker out.
    private var committedAhead: [CommittedEdit] = []
    private var rejections: [RejectedEdit] = []
    private var inFlight: Task<SyncReport, any Error>?

    /// Batch and page sizes are clamped to `1...500`.
    public init(actor: ActorID, transport: any SyncTransport, pushBatchSize: Int = 50, pullPageSize: Int = 200) {
        self.actor = actor
        self.transport = transport
        self.pushBatchSize = min(max(pushBatchSize, 1), 500)
        self.pullPageSize = min(max(pullPageSize, 1), 500)
    }

    /// Restores a replica saved with `persistentState()`. Returns nil if the state belongs
    /// to a different actor, or is internally inconsistent (an outbox entry from another
    /// actor, or a sequence number at or below one already used).
    public init?(actor: ActorID, transport: any SyncTransport, restoring state: ReplicaState,
                 pushBatchSize: Int = 50, pullPageSize: Int = 200) {
        let used = state.outbox.map(\.id) + state.committedAhead.map(\.mutation.id)
        guard state.actor == actor, used.allSatisfy({ $0.actor == actor && $0.clientSeq < state.nextSeq }) else {
            return nil
        }
        self.actor = actor
        self.transport = transport
        self.pushBatchSize = min(max(pushBatchSize, 1), 500)
        self.pullPageSize = min(max(pullPageSize, 1), 500)
        confirmed = state.confirmed
        nextSeq = state.nextSeq
        outbox = state.outbox
        committedAhead = state.committedAhead
        // Same rule as `bumpSequencePastAdmittedEdits()`, inlined: init is nonisolated.
        let admitted = state.confirmed.appliedThrough[actor] ?? 0
        if admitted < UInt64.max, nextSeq <= admitted { nextSeq = admitted + 1 }
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
    @discardableResult
    public func sync() async throws -> SyncReport {
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
        for _ in 0..<10_000 {
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
                // Stop at the head, or if the page made no progress.
                if confirmed.version >= head || confirmed.version == cursor { return }
            }
        }
    }

    private func dropPulledCommits() {
        committedAhead.removeAll { $0.seq <= confirmed.version }
        bumpSequencePastAdmittedEdits()
    }

    /// If the log already holds this actor's mutations up to some sequence number (for
    /// instance after a relaunch from an older save), never hand out those numbers again.
    /// A partial guard: it can't help for keys the replica uses before its first pull.
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
