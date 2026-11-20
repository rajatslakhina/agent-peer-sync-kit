@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

final class CommitLogTests: XCTestCase {
    private func makeLog(_ configuration: CommitLog.Configuration = .init()) -> CommitLog {
        CommitLog(schema: SampleWorkspace.schema, policies: SampleWorkspace.policies, configuration: configuration)
    }

    private func write(_ actor: ActorID, _ seq: UInt64, _ record: RecordID, _ ops: [FieldOp], base: UInt64 = 0) -> Mutation {
        Mutation(id: MutationID(actor: actor, clientSeq: seq), body: .write(record: record, ops: ops, baseVersion: base))
    }

    private func review(_ actor: ActorID, _ seq: UInt64, _ proposal: UInt64, _ decision: ReviewDecision) -> Mutation {
        Mutation(id: MutationID(actor: actor, clientSeq: seq), body: .review(proposal: proposal, decision: decision))
    }

    private func push(_ log: CommitLog, _ mutation: Mutation, as actor: ActorID? = nil) async -> MutationOutcome? {
        let results = await log.push([mutation], as: actor ?? mutation.id.actor)
        XCTAssertEqual(results.count, 1)
        return results.first?.outcome
    }

    private func votes(_ log: CommitLog, _ record: RecordID = Names.item1) async -> FieldValue? {
        await log.snapshot().records[record]?[Names.votes]
    }

    /// item-1 created by Asha at v1.
    private func seeded() async -> CommitLog {
        let log = makeLog()
        _ = await push(log, write(Names.asha, 1, Names.item1, [.set(Names.title, .text("Ship")), .add(Names.votes, 0)]))
        return log
    }

    // MARK: Idempotency and ordering

    func testRetryReplaysTheOriginalOutcomeAndAppliesNothingTwice() async {
        let log = await seeded()
        let vote = write(Names.ben, 1, Names.item1, [.add(Names.votes, 1)], base: 1)
        let first = await push(log, vote)
        let second = await push(log, vote)
        XCTAssertEqual(first, .committed(seq: 2))
        XCTAssertEqual(second, .committed(seq: 2))
        let votes = await votes(log)
        XCTAssertEqual(votes, .count(1))
        let head = await log.head
        XCTAssertEqual(head, 2)
    }

    func testReusedKeyWithADifferentBodyIsAConflictNotAReplay() async {
        let log = await seeded()
        _ = await push(log, write(Names.ben, 1, Names.item1, [.add(Names.votes, 1)], base: 1))
        let forged = await push(log, write(Names.ben, 1, Names.item1, [.add(Names.votes, 100)], base: 1))
        XCTAssertEqual(forged, .rejected(.idempotencyConflict))
        let votes = await votes(log)
        XCTAssertEqual(votes, .count(1))
    }

    func testSequenceGapIsRefusedAndRecordsNothing() async {
        let log = await seeded()
        let early = await push(log, write(Names.ben, 3, Names.item1, [.add(Names.votes, 1)], base: 1))
        XCTAssertEqual(early, .outOfOrder(expected: 1))
        let zero = await push(log, write(Names.ben, 0, Names.item1, [.add(Names.votes, 1)], base: 1))
        XCTAssertEqual(zero, .outOfOrder(expected: 1))
        let head = await log.head
        XCTAssertEqual(head, 1)
        // The gap left no trace: seq 1 is still accepted.
        let first = await push(log, write(Names.ben, 1, Names.item1, [.add(Names.votes, 1)], base: 1))
        XCTAssertEqual(first, .committed(seq: 2))
    }

    func testARejectionConsumesItsSequenceNumber() async {
        let log = await seeded()
        let refused = await push(log, write(Names.agent, 1, Names.item1, [.set(Names.done, .flag(true))], base: 1))
        XCTAssertEqual(refused, .rejected(.notPermitted(Names.done)))
        let next = await push(log, write(Names.agent, 2, Names.item1, [.set(Names.summary, .text("s"))], base: 1))
        XCTAssertEqual(next, .committed(seq: 2))
        // And the refusal is replayed, not re-evaluated, on retry.
        let retry = await push(log, write(Names.agent, 1, Names.item1, [.set(Names.done, .flag(true))], base: 1))
        XCTAssertEqual(retry, .rejected(.notPermitted(Names.done)))
    }

    func testCallerCannotSubmitAnotherActorsMutation() async {
        let log = await seeded()
        let spoofed = await push(log, write(Names.asha, 2, Names.item1, [.add(Names.votes, 1)], base: 1), as: Names.agent)
        XCTAssertEqual(spoofed, .rejected(.actorMismatch))
        // Nothing consumed for either actor.
        let asha = await push(log, write(Names.asha, 2, Names.item1, [.add(Names.votes, 1)], base: 1))
        XCTAssertEqual(asha, .committed(seq: 2))
        let agent = await push(log, write(Names.agent, 1, Names.item1, [.set(Names.summary, .text("s"))], base: 2))
        XCTAssertEqual(agent, .committed(seq: 3))
    }

    func testUnknownActorIsRefusedButARevokedActorsRetryStillLearnsTheTruth() async {
        let log = await seeded()
        guard let stranger = ActorID("stranger") else { return XCTFail("valid literal") }
        let unknown = await push(log, write(stranger, 1, Names.item1, [.add(Names.votes, 1)], base: 1))
        XCTAssertEqual(unknown, .rejected(.unknownActor))

        let summary = write(Names.agent, 1, Names.item1, [.set(Names.summary, .text("s"))], base: 1)
        let committed = await push(log, summary)
        XCTAssertEqual(committed, .committed(seq: 2))
        await log.setPolicy(nil, for: Names.agent)
        let retry = await push(log, summary)
        XCTAssertEqual(retry, .committed(seq: 2))
        let fresh = await push(log, write(Names.agent, 2, Names.item1, [.set(Names.summary, .text("t"))], base: 2))
        XCTAssertEqual(fresh, .rejected(.unknownActor))
    }

    func testRetryOlderThanTheRetentionWindowExpiresInsteadOfReapplying() async {
        let log = makeLog(.init(outcomeRetention: 2))
        _ = await push(log, write(Names.asha, 1, Names.item1, [.add(Names.votes, 0)]))
        for seq: UInt64 in 1...3 {
            _ = await push(log, write(Names.ben, seq, Names.item1, [.add(Names.votes, 1)], base: 1))
        }
        let expired = await push(log, write(Names.ben, 1, Names.item1, [.add(Names.votes, 1)], base: 1))
        XCTAssertEqual(expired, .rejected(.retryExpired))
        let retained = await push(log, write(Names.ben, 2, Names.item1, [.add(Names.votes, 1)], base: 1))
        XCTAssertEqual(retained, .committed(seq: 3))
        let votes = await votes(log)
        XCTAssertEqual(votes, .count(3))
    }

    func testOversizedBatchIsCutWithoutConsumingSequenceNumbers() async {
        let log = makeLog(.init(maxPushBatch: 2))
        let batch = (1...3).map { write(Names.asha, UInt64($0), Names.item1, [.add(Names.votes, 1)]) }
        let outcomes = await log.push(batch, as: Names.asha).map(\.outcome)
        XCTAssertEqual(outcomes, [.committed(seq: 1), .committed(seq: 2), .rejected(.batchTooLarge(limit: 2))])
        // The cut mutation was never looked at: the same key is admitted next time.
        let again = await push(log, batch[2])
        XCTAssertEqual(again, .committed(seq: 3))
    }

    func testReplayWindowKeepsAStableFixedSizeFingerprint() {
        // A known value, so a per-process seeded hash (like `Hasher`) would fail here.
        let mutation = write(Names.asha, 1, Names.item1, [.add(Names.votes, 1)])
        XCTAssertEqual(Fingerprint(mutation).hex, "8a32692bb50326605782e31149c66b1b")
        let other = write(Names.asha, 1, Names.item1, [.add(Names.votes, 2)])
        XCTAssertNotEqual(Fingerprint(mutation), Fingerprint(other))
        // What the window keeps per outcome is the 16-byte fingerprint plus the outcome,
        // nothing that holds the caller-sized body.
        XCTAssertEqual(MemoryLayout<Fingerprint>.size, 16)
        XCTAssertEqual(CommitLog.storedOutcomeSize,
                       MemoryLayout<Fingerprint>.size + MemoryLayout<MutationOutcome>.size)
    }

    // MARK: Review

    /// item-1 created at v1, the agent proposes a new title at v2.
    private func withProposal() async -> CommitLog {
        let log = await seeded()
        let proposed = await push(log, write(Names.agent, 1, Names.item1, [.set(Names.title, .text("Agent title"))], base: 1))
        XCTAssertEqual(proposed, .proposed(seq: 2))
        return log
    }

    func testAProposalChangesNothingUntilAccepted() async {
        let log = await withProposal()
        var snapshot = await log.snapshot()
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.title], .text("Ship"))
        XCTAssertEqual(snapshot.openProposals.map(\.seq), [2])

        let accepted = await push(log, review(Names.asha, 2, 2, .accept))
        XCTAssertEqual(accepted, .committed(seq: 3))
        snapshot = await log.snapshot()
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.title], .text("Agent title"))
        // Attributed to its author, not to the reviewer, at the resolution's version.
        XCTAssertEqual(snapshot.records[Names.item1]?.writes[Names.title], FieldWrite(version: 3, actor: Names.agent))
        XCTAssertTrue(snapshot.openProposals.isEmpty)
    }

    func testRejectingAProposalClosesItWithoutApplying() async {
        let log = await withProposal()
        let rejected = await push(log, review(Names.ben, 1, 2, .reject))
        XCTAssertEqual(rejected, .committed(seq: 3))
        let snapshot = await log.snapshot()
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.title], .text("Ship"))
        XCTAssertTrue(snapshot.openProposals.isEmpty)
        let again = await push(log, review(Names.asha, 2, 2, .accept))
        XCTAssertEqual(again, .rejected(.noSuchProposal(2)))
    }

    func testAcceptanceIsRecheckedAgainstTheCurrentStateNotTheProposalsState() async {
        let log = await withProposal()
        // While the proposal waits, Ben retitles the item himself.
        let human = await push(log, write(Names.ben, 1, Names.item1, [.set(Names.title, .text("Ben title"))], base: 2))
        XCTAssertEqual(human, .committed(seq: 3))
        let accepted = await push(log, review(Names.asha, 2, 2, .accept))
        XCTAssertEqual(accepted, .committed(seq: 4))
        let snapshot = await log.snapshot()
        XCTAssertEqual(snapshot.records[Names.item1]?[Names.title], .text("Ben title"))
        let resolution = await log.retainedEntries().last?.event
        XCTAssertEqual(resolution, .resolved(proposal: 2, resolution: .superseded(.staleWrite(Names.title, currentVersion: 3)),
                                             by: MutationID(actor: Names.asha, clientSeq: 2)))
    }

    func testAcceptanceUsesTheAuthorsCurrentGrant() async {
        let log = await withProposal()
        let downgraded = ActorPolicy(kind: .agent, directFields: [Names.summary], mayCreate: false,
                                     mayDelete: false, mayReview: false)
        await log.setPolicy(downgraded, for: Names.agent)
        _ = await push(log, review(Names.asha, 2, 2, .accept))
        var resolution = await log.retainedEntries().last?.event
        XCTAssertEqual(resolution, .resolved(proposal: 2, resolution: .superseded(.notPermitted(Names.title)),
                                             by: MutationID(actor: Names.asha, clientSeq: 2)))

        let revokedLog = await withProposal()
        await revokedLog.setPolicy(nil, for: Names.agent)
        _ = await push(revokedLog, review(Names.asha, 2, 2, .accept))
        resolution = await revokedLog.retainedEntries().last?.event
        XCTAssertEqual(resolution, .resolved(proposal: 2, resolution: .superseded(.unknownActor),
                                             by: MutationID(actor: Names.asha, clientSeq: 2)))
    }

    func testReviewPermissions() async {
        let log = await withProposal()
        let byAgent = await push(log, review(Names.agent, 2, 2, .accept))
        XCTAssertEqual(byAgent, .rejected(.reviewNotPermitted))
        let missing = await push(log, review(Names.asha, 2, 99, .accept))
        XCTAssertEqual(missing, .rejected(.noSuchProposal(99)))

        // Even a reviewer can't settle its own proposal.
        let reviewerProposes = ActorPolicy(kind: .device, directFields: [], reviewedFields: [Names.title],
                                           mayCreate: false, mayDelete: false, mayReview: true)
        await log.setPolicy(reviewerProposes, for: Names.ben)
        let own = await push(log, write(Names.ben, 1, Names.item1, [.set(Names.title, .text("mine"))], base: 2))
        XCTAssertEqual(own, .proposed(seq: 3))
        let selfReview = await push(log, review(Names.ben, 2, 3, .accept))
        XCTAssertEqual(selfReview, .rejected(.selfReview))
    }

    // MARK: Pull and compaction

    func testPullPagesAreClampedAndContiguous() async {
        let log = await seeded()
        for seq: UInt64 in 1...4 {
            _ = await push(log, write(Names.ben, seq, Names.item1, [.add(Names.votes, 1)], base: 1))
        }
        guard case let .entries(one, head) = await log.pull(after: 0, limit: 0) else { return XCTFail("expected entries") }
        XCTAssertEqual(one.map(\.seq), [1])
        XCTAssertEqual(head, 5)
        guard case let .entries(rest, _) = await log.pull(after: 2, limit: 100) else { return XCTFail("expected entries") }
        XCTAssertEqual(rest.map(\.seq), [3, 4, 5])
        guard case let .entries(none, _) = await log.pull(after: 5, limit: 100) else { return XCTFail("expected entries") }
        XCTAssertTrue(none.isEmpty)
    }

    func testCursorBehindCompactionOrAheadOfHeadGetsASnapshotWithOpenProposals() async {
        let log = await withProposal()
        await log.compact()
        let expected = await log.snapshot()
        let behind = await log.pull(after: 0, limit: 10)
        XCTAssertEqual(behind, .snapshot(expected))
        let ahead = await log.pull(after: 999, limit: 10)
        XCTAssertEqual(ahead, .snapshot(expected))
        XCTAssertEqual(expected.openProposals.map(\.seq), [2])
        // The proposal survives compaction and can still be accepted.
        let accepted = await push(log, review(Names.asha, 2, 2, .accept))
        XCTAssertEqual(accepted, .committed(seq: 3))
    }

    func testLogCompactsItselfToBoundMemory() async {
        let log = makeLog(.init(maxRetainedEntries: 3))
        _ = await push(log, write(Names.asha, 1, Names.item1, [.add(Names.votes, 0)]))
        // The 4th entry crosses the limit of 3: everything but the newest maxRetainedEntries / 2 (= 1) is folded.
        for seq: UInt64 in 1...3 {
            _ = await push(log, write(Names.ben, seq, Names.item1, [.add(Names.votes, 1)], base: 1))
        }
        let retained = await log.retainedEntries().count
        XCTAssertEqual(retained, 1)
        XCTAssertGreaterThan(retained, 0, "compaction keeps the newer half")
        // A cursor one entry behind is still served entries, not a snapshot.
        let head = await log.head
        guard case let .entries(page, _) = await log.pull(after: head - 1, limit: 10) else {
            return XCTFail("a slightly-behind replica should get entries")
        }
        XCTAssertEqual(page.map(\.seq), [head])
        let votes = await votes(log)
        XCTAssertEqual(votes, .count(3))
    }
}
