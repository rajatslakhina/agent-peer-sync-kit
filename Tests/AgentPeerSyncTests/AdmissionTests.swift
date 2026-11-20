@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

/// The server's admission rules, tested as a pure function.
final class AdmissionTests: XCTestCase {
    private let schema = SampleWorkspace.schema
    private let person = SampleWorkspace.personPolicy
    private let agent = SampleWorkspace.agentPolicy

    /// State at v`version` where item-1 exists, its title was last written by `titleAuthor` at v2.
    private func state(titleAuthor: ActorID = Names.asha, version: UInt64 = 5) -> Snapshot {
        var snapshot = Snapshot()
        let create = Mutation(id: MutationID(actor: Names.asha, clientSeq: 1),
                              body: .write(record: Names.item1, ops: [.set(Names.title, .text("a"))], baseVersion: 0))
        snapshot.apply(LogEntry(seq: 1, event: .committed(create)))
        let retitle = Mutation(id: MutationID(actor: titleAuthor, clientSeq: 9),
                               body: .write(record: Names.item1, ops: [.set(Names.title, .text("b"))], baseVersion: 1))
        snapshot.apply(LogEntry(seq: 2, event: .committed(retitle)))
        let filler = Mutation(id: MutationID(actor: Names.asha, clientSeq: 2),
                              body: .write(record: Names.item2, ops: [.set(Names.notes, .text("x"))], baseVersion: 2))
        for seq in 3...max(3, version) { snapshot.apply(LogEntry(seq: seq, event: .committed(filler))) }
        return snapshot
    }

    private func evaluate(_ ops: [FieldOp], record: RecordID = Names.item1, base: UInt64 = 1,
                          author: ActorID = Names.ben, policy: ActorPolicy? = nil, in snapshot: Snapshot? = nil) -> Admission {
        Admission.evaluate(record: record, ops: ops, baseVersion: base, author: author,
                           policy: policy ?? person, schema: schema, state: snapshot ?? state())
    }

    func testStaleWriteToGuardedFieldIsRejected() {
        XCTAssertEqual(evaluate([.set(Names.title, .text("c"))], base: 1),
                       .reject(.staleWrite(Names.title, currentVersion: 2)))
    }

    func testWriteBasedOnTheLatestVersionIsNotStale() {
        XCTAssertEqual(evaluate([.set(Names.title, .text("c"))], base: 2), .commit)
    }

    func testAuthorsOwnLaterWriteDoesNotMakeItsNextWriteStale() {
        // Ben wrote the title at v2; his next offline edit still has base 1. Not a conflict.
        XCTAssertEqual(evaluate([.set(Names.title, .text("c"))], base: 1, in: state(titleAuthor: Names.ben)), .commit)
    }

    func testLastWriterWinsFieldAcceptsAStaleBase() {
        XCTAssertEqual(evaluate([.set(Names.notes, .text("n"))], record: Names.item2, base: 0), .commit)
    }

    func testReviewedFieldMakesTheWholeMutationAProposal() {
        XCTAssertEqual(evaluate([.set(Names.summary, .text("s"))], base: 2, author: Names.agent, policy: agent), .commit)
        XCTAssertEqual(evaluate([.set(Names.summary, .text("s")), .set(Names.title, .text("t"))], base: 2,
                                author: Names.agent, policy: agent), .propose)
    }

    func testFieldOutsideTheGrantIsRejected() {
        XCTAssertEqual(evaluate([.set(Names.done, .flag(true))], base: 2, author: Names.agent, policy: agent),
                       .reject(.notPermitted(Names.done)))
        XCTAssertEqual(evaluate([.add(Names.votes, 1)], base: 2, author: Names.agent, policy: agent),
                       .reject(.notPermitted(Names.votes)))
    }

    func testFieldInBothGrantsIsTreatedAsReviewed() {
        let both = ActorPolicy(kind: .agent, directFields: [Names.title], reviewedFields: [Names.title],
                               mayCreate: false, mayDelete: false, mayReview: false)
        XCTAssertEqual(both.grant(for: Names.title), .reviewed)
        XCTAssertEqual(evaluate([.set(Names.title, .text("t"))], base: 2, author: Names.agent, policy: both), .propose)
    }

    func testUnknownFieldAndTypeMismatchesAreRejected() {
        let bogus = FieldName("colour")
        XCTAssertNotNil(bogus)
        guard let bogus else { return }
        XCTAssertEqual(evaluate([.set(bogus, .text("red"))], base: 2), .reject(.unknownField(bogus)))
        XCTAssertEqual(evaluate([.set(Names.votes, .count(5))], base: 2), .reject(.typeMismatch(Names.votes)))
        XCTAssertEqual(evaluate([.add(Names.title, 1)], base: 2), .reject(.typeMismatch(Names.title)))
        XCTAssertEqual(evaluate([.set(Names.done, .text("yes"))], base: 2), .reject(.typeMismatch(Names.done)))
    }

    func testTextLimitCountsUnicodeScalarsNotBytes() {
        // 120 scalars, 480 UTF-8 bytes: within the 120-scalar title limit.
        let atLimit = String(repeating: "🙂", count: 120)
        XCTAssertEqual(evaluate([.set(Names.title, .text(atLimit))], base: 2), .commit)
        XCTAssertEqual(evaluate([.set(Names.title, .text(atLimit + "a"))], base: 2),
                       .reject(.textTooLong(Names.title, limit: 120)))
    }

    func testDeleteRules() {
        XCTAssertEqual(evaluate([.delete], base: 2), .commit)
        XCTAssertEqual(evaluate([.delete, .set(Names.notes, .text("x"))], base: 2), .reject(.deleteMustBeAlone))
        XCTAssertEqual(evaluate([.delete], base: 2, author: Names.agent, policy: agent), .reject(.deleteNotPermitted))
        XCTAssertEqual(evaluate([.delete], record: Names.item3, base: 2), .reject(.noSuchRecord))
    }

    func testWritesToATombstonedRecordAreRejected() {
        var snapshot = state()
        let delete = Mutation(id: MutationID(actor: Names.asha, clientSeq: 3),
                              body: .write(record: Names.item1, ops: [.delete], baseVersion: 5))
        snapshot.apply(LogEntry(seq: 6, event: .committed(delete)))
        XCTAssertEqual(evaluate([.add(Names.votes, 1)], base: 6, in: snapshot), .reject(.recordDeleted))
        XCTAssertEqual(snapshot.liveRecords.map(\.id), [Names.item2])
    }

    func testCreationNeedsTheGrant() {
        XCTAssertEqual(evaluate([.set(Names.title, .text("new"))], record: Names.item3, base: 2), .commit)
        XCTAssertEqual(evaluate([.set(Names.summary, .text("new"))], record: Names.item3, base: 2,
                                author: Names.agent, policy: agent), .reject(.createNotPermitted))
    }

    func testShapeLimits() {
        XCTAssertEqual(evaluate([], base: 2), .reject(.emptyMutation))
        let tooMany = Array(repeating: FieldOp.add(Names.votes, 1), count: schema.maxOpsPerMutation + 1)
        XCTAssertEqual(evaluate(tooMany, base: 2), .reject(.tooManyOps(limit: schema.maxOpsPerMutation)))
        XCTAssertEqual(evaluate([.add(Names.votes, 1)], base: 99), .reject(.baseVersionAhead(head: 5)))
    }

    func testProposalQueueAppliesBackpressurePerAuthor() {
        var snapshot = state()
        let limit = agent.maxOpenProposals
        for index in 0..<limit {
            let proposal = Mutation(id: MutationID(actor: Names.agent, clientSeq: UInt64(index + 1)),
                                    body: .write(record: Names.item1, ops: [.set(Names.title, .text("p"))], baseVersion: 5))
            snapshot.apply(LogEntry(seq: UInt64(6 + index), event: .proposed(proposal)))
        }
        XCTAssertEqual(snapshot.openProposals.count, limit)
        let next = evaluate([.set(Names.notes, .text("n"))], base: snapshot.version, author: Names.agent,
                            policy: agent, in: snapshot)
        XCTAssertEqual(next, .reject(.proposalQueueFull(limit: limit)))
        // Direct writes are not held back by the queue.
        XCTAssertEqual(evaluate([.set(Names.summary, .text("s"))], base: snapshot.version, author: Names.agent,
                                policy: agent, in: snapshot), .commit)
    }

    func testPolicyAndSchemaClampTheirLimits() {
        let wild = ActorPolicy(kind: .agent, directFields: [], mayCreate: false, mayDelete: false,
                               mayReview: false, maxOpenProposals: -5)
        XCTAssertEqual(wild.maxOpenProposals, 1)
        let huge = Schema(fields: [Names.title: .text(maxLength: .max, conflict: .rejectStale)], maxOpsPerMutation: .min)
        XCTAssertEqual(huge.maxOpsPerMutation, 1)
        XCTAssertEqual(huge.fields[Names.title], .text(maxLength: Schema.maxTextLimit, conflict: .rejectStale))
    }

    func testCounterAddSaturatesInsteadOfTrapping() {
        var records: [RecordID: Record] = [:]
        Snapshot.applyOps([.add(Names.votes, .max), .add(Names.votes, 1)], to: Names.item1, by: Names.asha, at: 1, in: &records)
        XCTAssertEqual(records[Names.item1]?[Names.votes], .count(.max))
        Snapshot.applyOps([.add(Names.votes, .min), .add(Names.votes, .min)], to: Names.item2, by: Names.asha, at: 2, in: &records)
        XCTAssertEqual(records[Names.item2]?[Names.votes], .count(.min))
        XCTAssertEqual(Saturating.add(-3, 5), 2)
    }

    func testIdentifierValidation() {
        XCTAssertNil(ActorID(""))
        XCTAssertNil(ActorID("Phone"))
        XCTAssertNil(ActorID(String(repeating: "a", count: 65)))
        XCTAssertNotNil(ActorID(String(repeating: "a", count: 64)))
        XCTAssertNil(RecordID("item 1"))
        XCTAssertNil(FieldName("tïtle"))
        XCTAssertThrowsError(try JSONDecoder().decode(ActorID.self, from: Data("\"BAD\"".utf8)))
    }

    func testSampleNamesAreValidLiterals() {
        // Constructs every `Names` literal; a typo would fail here instead of at a call site.
        let fields = [Names.title, Names.notes, Names.done, Names.votes, Names.summary]
        XCTAssertEqual(Set(fields).count, 5)
        XCTAssertEqual(Set([Names.asha, Names.ben, Names.agent]).count, 3)
        XCTAssertEqual(Set([Names.item1, Names.item2, Names.item3]).count, 3)
    }
}
