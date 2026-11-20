import AgentPeerSync
import Foundation

/// Field, actor and record names used by the sample workspace.
public enum Names {
    public static let title = literal(FieldName("title"), "title")
    public static let notes = literal(FieldName("notes"), "notes")
    public static let done = literal(FieldName("done"), "done")
    public static let votes = literal(FieldName("votes"), "votes")
    public static let summary = literal(FieldName("summary"), "summary")

    public static let asha = literal(ActorID("phone-asha"), "phone-asha")
    public static let ben = literal(ActorID("phone-ben"), "phone-ben")
    public static let agent = literal(ActorID("notes-agent"), "notes-agent")

    public static let item1 = literal(RecordID("item-1"), "item-1")
    public static let item2 = literal(RecordID("item-2"), "item-2")
    public static let item3 = literal(RecordID("item-3"), "item-3")

    /// Provably non-nil: every literal above is 1–64 characters of `a–z 0–9 -`, which
    /// `IdentifierRule` accepts, and `AdmissionTests.testSampleNamesAreValidLiterals`
    /// constructs each one. The precondition
    /// documents that instead of hiding it behind a `!`.
    static func literal<T>(_ value: T?, _ text: String) -> T {
        guard let value else { preconditionFailure("\(text) is not a valid identifier literal") }
        return value
    }
}

/// The sample domain: a shared list of meeting action items.
public enum SampleWorkspace {
    public static let schema = Schema(fields: [
        Names.title: .text(maxLength: 120, conflict: .rejectStale),
        Names.notes: .text(maxLength: 2_000, conflict: .lastWriterWins),
        Names.done: .flag(conflict: .lastWriterWins),
        Names.votes: .counter,
        Names.summary: .text(maxLength: 500, conflict: .lastWriterWins),
    ])

    public static let personPolicy = ActorPolicy(
        kind: .device, directFields: Set(schema.fields.keys),
        mayCreate: true, mayDelete: true, mayReview: true)

    /// The agent writes its own `summary` field directly, may only *propose* changes to
    /// human-authored text, and can't create, delete, vote, tick items off or review.
    public static let agentPolicy = ActorPolicy(
        kind: .agent, directFields: [Names.summary], reviewedFields: [Names.title, Names.notes],
        mayCreate: false, mayDelete: false, mayReview: false, maxOpenProposals: 4)

    public static var policies: [ActorID: ActorPolicy] {
        [Names.asha: personPolicy, Names.ben: personPolicy, Names.agent: agentPolicy]
    }
}

/// One server, three replicas (two phones and an agent), each with its own transport.
public final class Workspace: Sendable {
    public let server: CommitLog
    public let ashaLink: LocalTransport
    public let benLink: LocalTransport
    public let agentLink: LocalTransport
    public let asha: Replica
    public let ben: Replica
    public let agent: Replica

    public init(configuration: CommitLog.Configuration = .init()) {
        server = CommitLog(schema: SampleWorkspace.schema, policies: SampleWorkspace.policies,
                           configuration: configuration)
        ashaLink = LocalTransport(server: server, authenticatedAs: Names.asha)
        benLink = LocalTransport(server: server, authenticatedAs: Names.ben)
        agentLink = LocalTransport(server: server, authenticatedAs: Names.agent)
        asha = Replica(actor: Names.asha, transport: ashaLink)
        ben = Replica(actor: Names.ben, transport: benLink)
        agent = Replica(actor: Names.agent, transport: agentLink)
    }

    public var replicas: [Replica] { [asha, ben, agent] }

    public struct Convergence: Hashable, Sendable {
        public struct Row: Hashable, Sendable {
            public let actor: ActorID
            public let cursor: UInt64
            public let pendingEdits: Int
            public let matchesServer: Bool
        }
        public let serverHead: UInt64
        public let rows: [Row]
        public var converged: Bool { rows.allSatisfy { $0.matchesServer && $0.pendingEdits == 0 } }
    }

    /// Compares each replica's confirmed state with the server's, without syncing.
    public func convergence() async -> Convergence {
        await Self.convergence(of: replicas, with: server)
    }

    /// The same check for any set of replicas against any log.
    public static func convergence(of replicas: [Replica], with server: CommitLog) async -> Convergence {
        let truth = await server.snapshot()
        var rows: [Convergence.Row] = []
        for replica in replicas {
            let view = await replica.view()
            rows.append(.init(actor: view.actor, cursor: view.confirmed.version,
                              pendingEdits: view.outbox.count, matchesServer: view.confirmed == truth))
        }
        return Convergence(serverHead: truth.version, rows: rows)
    }
}
