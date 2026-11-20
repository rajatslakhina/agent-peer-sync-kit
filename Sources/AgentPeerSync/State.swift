import Foundation

/// Saturating arithmetic, so no value reachable through the public API can trap.
public enum Saturating {
    public static func add(_ lhs: Int64, _ rhs: Int64) -> Int64 {
        let (sum, overflow) = lhs.addingReportingOverflow(rhs)
        guard overflow else { return sum }
        return rhs > 0 ? .max : .min
    }
}

public struct FieldWrite: Hashable, Sendable, Codable {
    /// The log sequence of the write that last changed this field.
    public let version: UInt64
    public let actor: ActorID
}

public struct Record: Hashable, Sendable, Codable {
    public internal(set) var fields: [FieldName: FieldValue] = [:]
    public internal(set) var writes: [FieldName: FieldWrite] = [:]
    public internal(set) var isDeleted = false

    public subscript(field: FieldName) -> FieldValue? { fields[field] }
}

public struct Proposal: Hashable, Sendable, Codable {
    /// The log sequence the proposal was recorded at. Reviews refer to it by this number.
    public let seq: UInt64
    public let mutation: Mutation
    public var author: ActorID { mutation.id.actor }
}

public enum Resolution: Hashable, Sendable, Codable {
    case applied
    case rejected
    /// The reviewer accepted, but the proposal no longer applied to the current state
    /// (the record was deleted, a guarded field moved on, or the author lost the grant).
    /// It is closed without changing anything.
    case superseded(RejectionReason)
}

public enum LogEvent: Hashable, Sendable, Codable {
    case committed(Mutation)
    case proposed(Mutation)
    case resolved(proposal: UInt64, resolution: Resolution, by: MutationID)
}

public struct LogEntry: Hashable, Sendable, Codable, Identifiable {
    public let seq: UInt64
    public let event: LogEvent
    public var id: UInt64 { seq }
}

/// The shared state as of `version`: every record plus every open proposal.
///
/// The server and every replica build it with the same reducer, `applying(_:)`. Conflict
/// resolution happens once, on the server, when an entry is admitted to the log; replicas
/// replay admitted entries and never re-decide anything. That is what makes convergence a
/// property of the log rather than of each client's merge code.
public struct Snapshot: Hashable, Sendable, Codable {
    public internal(set) var version: UInt64 = 0
    public internal(set) var records: [RecordID: Record] = [:]
    public internal(set) var proposals: [UInt64: Proposal] = [:]
    /// For each actor, the highest client sequence number of its mutations that appear in
    /// the log. Derived by the reducer like everything else here, so it survives compaction
    /// and replicas agree on it. A replica uses it to tell "the server already has this
    /// edit, I just never heard back" from "the server hasn't seen this edit yet".
    public internal(set) var appliedThrough: [ActorID: UInt64] = [:]

    public init() {}

    /// Records that exist and are not tombstoned, sorted by id.
    public var liveRecords: [(id: RecordID, record: Record)] {
        records.filter { !$0.value.isDeleted }
            .sorted { $0.key < $1.key }
            .map { (id: $0.key, record: $0.value) }
    }

    /// Open proposals, oldest first.
    public var openProposals: [Proposal] {
        proposals.values.sorted { $0.seq < $1.seq }
    }

    /// Applies one admitted log entry if it is exactly the next one (`version + 1`).
    /// An entry at or below the current version is ignored, so replaying an overlapping
    /// page is harmless; an entry past a gap is refused, so a replica never skips history.
    /// Returns whether the entry was applied.
    @discardableResult
    public mutating func apply(_ entry: LogEntry) -> Bool {
        guard version < UInt64.max, entry.seq == version + 1 else { return false }
        let author: MutationID
        switch entry.event {
        case let .committed(mutation), let .proposed(mutation): author = mutation.id
        case let .resolved(_, _, by): author = by
        }
        appliedThrough[author.actor] = max(appliedThrough[author.actor] ?? 0, author.clientSeq)
        switch entry.event {
        case let .committed(mutation):
            if case let .write(record, ops, _) = mutation.body {
                Self.applyOps(ops, to: record, by: mutation.id.actor, at: entry.seq, in: &records)
            }
        case let .proposed(mutation):
            proposals[entry.seq] = Proposal(seq: entry.seq, mutation: mutation)
        case let .resolved(proposalSeq, resolution, _):
            if let proposal = proposals.removeValue(forKey: proposalSeq),
               resolution == .applied,
               case let .write(record, ops, _) = proposal.mutation.body {
                Self.applyOps(ops, to: record, by: proposal.author, at: entry.seq, in: &records)
            }
        }
        version = entry.seq
        return true
    }

    /// The one place record state changes. Used for admitted entries and, by `Replica`, for
    /// the optimistic overlay of edits the server hasn't answered yet.
    static func applyOps(_ ops: [FieldOp], to id: RecordID, by actor: ActorID, at seq: UInt64,
                         in records: inout [RecordID: Record]) {
        var record = records[id] ?? Record()
        for op in ops {
            switch op {
            case let .set(field, value):
                record.fields[field] = value
                record.writes[field] = FieldWrite(version: seq, actor: actor)
            case let .add(field, delta):
                var current: Int64 = 0
                if case let .count(existing)? = record.fields[field] { current = existing }
                record.fields[field] = .count(Saturating.add(current, delta))
                record.writes[field] = FieldWrite(version: seq, actor: actor)
            case .delete:
                record.fields = [:]
                record.writes = [:]
                record.isDeleted = true
            }
        }
        records[id] = record
    }
}
