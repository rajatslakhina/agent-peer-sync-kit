import Foundation

public enum PullResponse: Hashable, Sendable, Codable {
    /// Entries after the requested cursor, in order, plus the server's head at the time.
    case entries([LogEntry], head: UInt64)
    /// The cursor can't be served from the log (it was compacted away, or it is ahead of the
    /// server's head), so the replica must replace its confirmed state with this snapshot.
    case snapshot(Snapshot)
}

/// The server-authoritative commit log. Every actor, human device or agent, reaches shared
/// state only through `push`, and every replica learns about shared state only through `pull`.
///
/// `push` and `pull` contain no suspension points, so actor isolation makes each one atomic:
/// there is no reentrancy window between checking a sequence number and recording it.
public actor CommitLog {
    public struct Configuration: Hashable, Sendable {
        /// How many outcomes per actor are kept for idempotent replay. A retry older than
        /// the window gets `.retryExpired` instead of a second application.
        public var outcomeRetention: Int
        /// Entries kept before the log compacts itself into a snapshot. Bounds memory; a
        /// replica whose cursor falls behind the compaction point is sent the snapshot.
        public var maxRetainedEntries: Int
        public var maxPullPage: Int

        /// Values are clamped: retention to `1...100_000`, retained entries to
        /// `1...1_000_000`, page size to `1...10_000`.
        public init(outcomeRetention: Int = 1_024, maxRetainedEntries: Int = 10_000, maxPullPage: Int = 500) {
            self.outcomeRetention = min(max(outcomeRetention, 1), 100_000)
            self.maxRetainedEntries = min(max(maxRetainedEntries, 1), 1_000_000)
            self.maxPullPage = min(max(maxPullPage, 1), 10_000)
        }
    }

    private struct StoredOutcome {
        let mutation: Mutation
        let outcome: MutationOutcome
    }

    private let schema: Schema
    private let configuration: Configuration
    private var policies: [ActorID: ActorPolicy]
    private var state = Snapshot()
    /// State as of the last compaction; `entries` holds everything after it, contiguously.
    private var base = Snapshot()
    private var entries: [LogEntry] = []
    private var lastSeq: [ActorID: UInt64] = [:]
    private var outcomes: [ActorID: [UInt64: StoredOutcome]] = [:]

    public init(schema: Schema, policies: [ActorID: ActorPolicy], configuration: Configuration = .init()) {
        self.schema = schema
        self.policies = policies
        self.configuration = configuration
    }

    public var head: UInt64 { state.version }
    public func snapshot() -> Snapshot { state }
    public func policy(for actor: ActorID) -> ActorPolicy? { policies[actor] }
    /// The retained (uncompacted) log, oldest first.
    public func retainedEntries() -> [LogEntry] { entries }

    /// Grant, change or revoke (`nil`) an actor's policy. Takes effect for the next mutation,
    /// including the re-check of that actor's open proposals when a reviewer accepts one.
    public func setPolicy(_ policy: ActorPolicy?, for actor: ActorID) {
        policies[actor] = policy
    }

    // MARK: Push

    /// Admits a batch from the authenticated `caller`, in order, and returns one result per
    /// mutation in the same order.
    public func push(_ batch: [Mutation], as caller: ActorID) -> [PushResult] {
        batch.map { PushResult(id: $0.id, outcome: admit($0, caller: caller)) }
    }

    private func admit(_ mutation: Mutation, caller: ActorID) -> MutationOutcome {
        // A caller may only submit its own mutations. Not recorded, no sequence number consumed.
        guard mutation.id.actor == caller else { return .rejected(.actorMismatch) }

        let last = lastSeq[caller] ?? 0
        let seq = mutation.id.clientSeq
        guard seq > 0 else { return last < UInt64.max ? .outOfOrder(expected: last + 1) : .rejected(.logFull) }
        if seq <= last {
            // A retry: replay what happened the first time instead of applying it again.
            // Checked before the policy, so a retry still learns the truth after a revocation.
            guard let stored = outcomes[caller]?[seq] else { return .rejected(.retryExpired) }
            return stored.mutation == mutation ? stored.outcome : .rejected(.idempotencyConflict)
        }
        // An unknown or revoked actor gets no new sequence numbers.
        guard let policy = policies[caller] else { return .rejected(.unknownActor) }
        // `seq > last` here, so `last < UInt64.max` and `last + 1` cannot overflow.
        guard seq == last + 1 else { return .outOfOrder(expected: last + 1) }

        let outcome = decide(mutation, author: caller, policy: policy)
        // Every admitted sequence number is consumed, rejections included, so a client
        // never has to resubmit a refused edit to make progress past it.
        lastSeq[caller] = seq
        var stored = outcomes[caller] ?? [:]
        stored[seq] = StoredOutcome(mutation: mutation, outcome: outcome)
        let retention = UInt64(configuration.outcomeRetention)
        if seq > retention { stored.removeValue(forKey: seq - retention) }
        outcomes[caller] = stored
        return outcome
    }

    private func decide(_ mutation: Mutation, author: ActorID, policy: ActorPolicy) -> MutationOutcome {
        switch mutation.body {
        case let .write(record, ops, baseVersion):
            switch Admission.evaluate(record: record, ops: ops, baseVersion: baseVersion, author: author,
                                      policy: policy, schema: schema, state: state) {
            case .commit:
                return append(.committed(mutation)).map { .committed(seq: $0) } ?? .rejected(.logFull)
            case .propose:
                return append(.proposed(mutation)).map { .proposed(seq: $0) } ?? .rejected(.logFull)
            case let .reject(reason):
                return .rejected(reason)
            }

        case let .review(proposalSeq, decision):
            guard policy.mayReview else { return .rejected(.reviewNotPermitted) }
            guard let proposal = state.proposals[proposalSeq] else { return .rejected(.noSuchProposal(proposalSeq)) }
            guard proposal.author != author else { return .rejected(.selfReview) }
            let resolution: Resolution
            switch decision {
            case .reject:
                resolution = .rejected
            case .accept:
                resolution = recheck(proposal)
            }
            let event = LogEvent.resolved(proposal: proposalSeq, resolution: resolution, by: mutation.id)
            return append(event).map { .committed(seq: $0) } ?? .rejected(.logFull)
        }
    }

    /// Acceptance is judged against the state *now*, not the state the proposal was made in:
    /// a human may have edited a guarded field, deleted the record, or revoked the agent's
    /// grant while the proposal waited.
    private func recheck(_ proposal: Proposal) -> Resolution {
        guard case let .write(record, ops, baseVersion) = proposal.mutation.body else {
            return .superseded(.emptyMutation)
        }
        guard let authorPolicy = policies[proposal.author] else { return .superseded(.unknownActor) }
        switch Admission.evaluate(record: record, ops: ops, baseVersion: baseVersion, author: proposal.author,
                                  policy: authorPolicy, schema: schema, state: state, forAcceptance: true) {
        case .commit, .propose: return .applied
        case let .reject(reason): return .superseded(reason)
        }
    }

    /// Appends an event at `head + 1`. Returns nil only if the 64-bit sequence is exhausted.
    private func append(_ event: LogEvent) -> UInt64? {
        guard state.version < UInt64.max else { return nil }
        let entry = LogEntry(seq: state.version + 1, event: event)
        _ = state.apply(entry) // always applies: the seq is exactly version + 1
        entries.append(entry)
        if entries.count > configuration.maxRetainedEntries { compact() }
        return entry.seq
    }

    // MARK: Pull

    public func pull(after cursor: UInt64, limit: Int) -> PullResponse {
        guard cursor >= base.version, cursor <= state.version else { return .snapshot(state) }
        // `cursor - base.version <= entries.count` because entries are contiguous after base.
        guard let start = Int(exactly: cursor - base.version), start <= entries.count else {
            return .snapshot(state)
        }
        let pageSize = min(max(limit, 1), configuration.maxPullPage)
        let end = start + min(pageSize, entries.count - start)
        return .entries(Array(entries[start..<end]), head: state.version)
    }

    /// Folds every retained entry into the base snapshot. Open proposals live in the
    /// snapshot, so they survive compaction.
    public func compact() {
        base = state
        entries.removeAll(keepingCapacity: true)
    }
}
