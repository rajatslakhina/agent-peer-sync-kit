import Foundation

public enum ConflictPolicy: String, Hashable, Sendable, Codable {
    /// The write the server admits last wins. Right for fields where losing an older value is cheap.
    case lastWriterWins
    /// A write is refused if another actor changed the field after the writer's base version.
    /// Right for fields where a silent overwrite would destroy someone's work.
    case rejectStale
}

public enum FieldKind: Hashable, Sendable, Codable {
    /// Text of at most `maxLength` Unicode scalars (the unit JSON Schema's `maxLength` counts).
    case text(maxLength: Int, conflict: ConflictPolicy)
    case flag(conflict: ConflictPolicy)
    /// Changed only by `FieldOp.add`, so concurrent changes merge instead of conflicting.
    case counter
}

public struct Schema: Hashable, Sendable, Codable {
    public static let maxTextLimit = 65_536
    public static let maxOpsLimit = 64

    public let fields: [FieldName: FieldKind]
    public let maxOpsPerMutation: Int

    /// Text limits are clamped to `1...maxTextLimit`, op limits to `1...maxOpsLimit`.
    public init(fields: [FieldName: FieldKind], maxOpsPerMutation: Int = 16) {
        self.fields = fields.mapValues { kind in
            if case let .text(limit, conflict) = kind {
                return .text(maxLength: min(max(limit, 1), Self.maxTextLimit), conflict: conflict)
            }
            return kind
        }
        self.maxOpsPerMutation = min(max(maxOpsPerMutation, 1), Self.maxOpsLimit)
    }
}

public enum ActorKind: String, Hashable, Sendable, Codable {
    case device
    case agent
}

/// What one actor may do. The server holds these; a client never sends its own.
public struct ActorPolicy: Hashable, Sendable, Codable {
    public static let maxOpenProposalsLimit = 1_000

    public let kind: ActorKind
    /// Fields this actor's writes change immediately.
    public let directFields: Set<FieldName>
    /// Fields this actor may only propose. A write that touches any of them becomes a
    /// proposal as a whole and changes nothing until another actor accepts it.
    public let reviewedFields: Set<FieldName>
    public let mayCreate: Bool
    public let mayDelete: Bool
    public let mayReview: Bool
    /// Backpressure on proposals: beyond this many open ones, new proposals are refused.
    public let maxOpenProposals: Int

    /// A field listed in both sets is treated as reviewed (the stricter grant wins).
    /// `maxOpenProposals` is clamped to `1...maxOpenProposalsLimit`.
    public init(kind: ActorKind, directFields: Set<FieldName>, reviewedFields: Set<FieldName> = [],
                mayCreate: Bool, mayDelete: Bool, mayReview: Bool, maxOpenProposals: Int = 32) {
        self.kind = kind
        self.reviewedFields = reviewedFields
        self.directFields = directFields.subtracting(reviewedFields)
        self.mayCreate = mayCreate
        self.mayDelete = mayDelete
        self.mayReview = mayReview
        self.maxOpenProposals = min(max(maxOpenProposals, 1), Self.maxOpenProposalsLimit)
    }

    public enum Grant: Hashable, Sendable { case direct, reviewed }

    public func grant(for field: FieldName) -> Grant? {
        if reviewedFields.contains(field) { return .reviewed }
        if directFields.contains(field) { return .direct }
        return nil
    }
}

/// The server's admission decision for one write. A pure function of the write, the
/// author's policy, the schema and the current state, so it is tested without an actor.
enum Admission: Equatable {
    case commit
    case propose
    case reject(RejectionReason)

    /// - Parameter forAcceptance: true when re-checking a proposal a reviewer just accepted.
    ///   Review requirements and the proposal queue no longer apply, but every other rule is
    ///   checked again against the *current* state and the author's *current* policy.
    static func evaluate(record: RecordID, ops: [FieldOp], baseVersion: UInt64, author: ActorID,
                         policy: ActorPolicy, schema: Schema, state: Snapshot,
                         forAcceptance: Bool = false) -> Admission {
        guard !ops.isEmpty else { return .reject(.emptyMutation) }
        guard ops.count <= schema.maxOpsPerMutation else {
            return .reject(.tooManyOps(limit: schema.maxOpsPerMutation))
        }
        guard baseVersion <= state.version else { return .reject(.baseVersionAhead(head: state.version)) }

        let existing = state.records[record]
        if existing?.isDeleted == true { return .reject(.recordDeleted) }

        if ops.contains(.delete) {
            guard ops.count == 1 else { return .reject(.deleteMustBeAlone) }
            guard policy.mayDelete else { return .reject(.deleteNotPermitted) }
            guard existing != nil else { return .reject(.noSuchRecord) }
            return .commit
        }
        if existing == nil, !policy.mayCreate { return .reject(.createNotPermitted) }

        var needsReview = false
        for op in ops {
            let field: FieldName
            switch op {
            case let .set(name, value):
                field = name
                guard let kind = schema.fields[name] else { return .reject(.unknownField(name)) }
                switch (kind, value) {
                case let (.text(limit, _), .text(text)):
                    if text.unicodeScalars.count > limit { return .reject(.textTooLong(name, limit: limit)) }
                case (.flag, .flag):
                    break
                default:
                    return .reject(.typeMismatch(name))
                }
            case let .add(name, _):
                field = name
                guard let kind = schema.fields[name] else { return .reject(.unknownField(name)) }
                guard case .counter = kind else { return .reject(.typeMismatch(name)) }
            case .delete:
                return .reject(.deleteMustBeAlone) // unreachable: handled above
            }

            switch policy.grant(for: field) {
            case nil: return .reject(.notPermitted(field))
            case .reviewed?: needsReview = true
            case .direct?: break
            }

            if case let .set(name, _) = op, Self.conflictPolicy(of: name, in: schema) == .rejectStale,
               let write = existing?.writes[name], write.version > baseVersion, write.actor != author {
                return .reject(.staleWrite(name, currentVersion: write.version))
            }
        }

        if forAcceptance { return .commit }
        guard needsReview else { return .commit }
        let open = state.proposals.values.reduce(0) { $1.author == author ? $0 + 1 : $0 }
        guard open < policy.maxOpenProposals else {
            return .reject(.proposalQueueFull(limit: policy.maxOpenProposals))
        }
        return .propose
    }

    static func conflictPolicy(of field: FieldName, in schema: Schema) -> ConflictPolicy? {
        switch schema.fields[field] {
        case let .text(_, conflict)?: return conflict
        case let .flag(conflict)?: return conflict
        case .counter?, nil: return nil
        }
    }
}
