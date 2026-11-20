import Foundation

// MARK: - Identifiers

/// Shared validation for every identifier that crosses the wire: 1–64 ASCII characters
/// from `a–z 0–9 - _ .`. Identifiers end up in log entries, tool schemas and UI, so an
/// unbounded or arbitrary string here would be an unbounded input everywhere else.
enum IdentifierRule {
    static let maxLength = 64

    static func isValid(_ raw: String) -> Bool {
        let scalars = raw.unicodeScalars
        guard !scalars.isEmpty, scalars.count <= maxLength else { return false }
        return scalars.allSatisfy { scalar in
            switch scalar {
            case "a"..."z", "0"..."9", "-", "_", ".": return true
            default: return false
            }
        }
    }
}

/// Who wrote a mutation. The server resolves an actor's kind and permissions from its own
/// registry; nothing the client sends can claim a different kind.
public struct ActorID: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public let rawValue: String
    public init?(_ raw: String) {
        guard IdentifierRule.isValid(raw) else { return nil }
        rawValue = raw
    }
    public var description: String { rawValue }
    public static func < (lhs: ActorID, rhs: ActorID) -> Bool { lhs.rawValue < rhs.rawValue }

    public init(from decoder: Decoder) throws {
        let raw = try decoder.singleValueContainer().decode(String.self)
        guard let value = ActorID(raw) else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "invalid actor id"))
        }
        self = value
    }
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}

public struct RecordID: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public let rawValue: String
    public init?(_ raw: String) {
        guard IdentifierRule.isValid(raw) else { return nil }
        rawValue = raw
    }
    public var description: String { rawValue }
    public static func < (lhs: RecordID, rhs: RecordID) -> Bool { lhs.rawValue < rhs.rawValue }

    public init(from decoder: Decoder) throws {
        let raw = try decoder.singleValueContainer().decode(String.self)
        guard let value = RecordID(raw) else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "invalid record id"))
        }
        self = value
    }
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}

public struct FieldName: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public let rawValue: String
    public init?(_ raw: String) {
        guard IdentifierRule.isValid(raw) else { return nil }
        rawValue = raw
    }
    public var description: String { rawValue }
    public static func < (lhs: FieldName, rhs: FieldName) -> Bool { lhs.rawValue < rhs.rawValue }

    public init(from decoder: Decoder) throws {
        let raw = try decoder.singleValueContainer().decode(String.self)
        guard let value = FieldName(raw) else {
            throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "invalid field name"))
        }
        self = value
    }
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(rawValue)
    }
}

// MARK: - Values and operations

public enum FieldValue: Hashable, Sendable, Codable {
    case text(String)
    case flag(Bool)
    case count(Int64)
}

/// One change to one record. A mutation's ops are applied atomically: all or none.
public enum FieldOp: Hashable, Sendable, Codable {
    /// Overwrite a text or flag field. Conflicts are settled by the field's `ConflictPolicy`.
    case set(FieldName, FieldValue)
    /// Add to a counter field. Increments commute, so concurrent adds from offline replicas
    /// all count; the sum saturates at `Int64.min`/`Int64.max` instead of trapping.
    case add(FieldName, Int64)
    /// Tombstone the record. Later writes to a tombstoned record are rejected.
    case delete
}

/// The idempotency key: one actor's monotonically increasing client sequence number.
/// The server accepts sequence `n + 1` after `n`, replays the stored outcome for a
/// repeated `n`, and refuses a gap.
public struct MutationID: Hashable, Comparable, Sendable, Codable, CustomStringConvertible {
    public let actor: ActorID
    public let clientSeq: UInt64
    public init(actor: ActorID, clientSeq: UInt64) {
        self.actor = actor
        self.clientSeq = clientSeq
    }
    public var description: String { "\(actor)#\(clientSeq)" }
    public static func < (lhs: MutationID, rhs: MutationID) -> Bool {
        (lhs.actor, lhs.clientSeq) < (rhs.actor, rhs.clientSeq)
    }
}

public enum ReviewDecision: String, Hashable, Sendable, Codable {
    case accept
    case reject
}

public enum MutationBody: Hashable, Sendable, Codable {
    /// Change a record. `baseVersion` is the server version the author had pulled when it
    /// wrote the change; stale-write detection compares against it.
    case write(record: RecordID, ops: [FieldOp], baseVersion: UInt64)
    /// Settle an open proposal. Review decisions sync like any other edit, so a reviewer
    /// can approve offline and the decision is ordered and deduplicated the same way.
    case review(proposal: UInt64, decision: ReviewDecision)
}

public struct Mutation: Hashable, Sendable, Codable {
    public let id: MutationID
    public let body: MutationBody
    public init(id: MutationID, body: MutationBody) {
        self.id = id
        self.body = body
    }
}

// MARK: - Outcomes

public enum RejectionReason: Hashable, Sendable, Codable {
    case unknownActor
    /// The mutation's author is not the authenticated caller.
    case actorMismatch
    case unknownField(FieldName)
    /// The op doesn't fit the field's kind (e.g. `add` on a text field).
    case typeMismatch(FieldName)
    case textTooLong(FieldName, limit: Int)
    case notPermitted(FieldName)
    case deleteNotPermitted
    /// A delete must be the mutation's only op.
    case deleteMustBeAlone
    case noSuchRecord
    case createNotPermitted
    case emptyMutation
    case tooManyOps(limit: Int)
    case recordDeleted
    /// A `rejectStale` field was written by another actor after this mutation's base version.
    case staleWrite(FieldName, currentVersion: UInt64)
    case reviewNotPermitted
    /// An actor may not settle its own proposal.
    case selfReview
    case noSuchProposal(UInt64)
    case proposalQueueFull(limit: Int)
    /// The same idempotency key arrived with a different body.
    case idempotencyConflict
    /// The key is older than the server's retained outcome window; the outcome is gone.
    case retryExpired
    case baseVersionAhead(head: UInt64)
    /// The push carried more mutations than the server admits in one batch.
    case batchTooLarge(limit: Int)
    case logFull
}

public enum MutationOutcome: Hashable, Sendable, Codable {
    /// Applied to the shared state at this log sequence.
    case committed(seq: UInt64)
    /// Recorded as a proposal at this log sequence; it changes nothing until a reviewer accepts it.
    case proposed(seq: UInt64)
    case rejected(RejectionReason)
    /// Sequence gap: the server expects `expected` next. Nothing was recorded.
    case outOfOrder(expected: UInt64)
}

public struct PushResult: Hashable, Sendable, Codable {
    public let id: MutationID
    public let outcome: MutationOutcome
    public init(id: MutationID, outcome: MutationOutcome) {
        self.id = id
        self.outcome = outcome
    }
}
