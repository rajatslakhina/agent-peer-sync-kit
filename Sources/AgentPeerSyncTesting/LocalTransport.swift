import AgentPeerSync
import Foundation

public struct TransportFailure: Error, Hashable, Sendable {
    public let reason: String
    public init(_ reason: String) { self.reason = reason }
}

/// An in-process transport to a `CommitLog`, authenticated as one actor, with switchable
/// connectivity and one-shot faults for the failure modes a real network produces.
public actor LocalTransport: SyncTransport {
    public enum Fault: Hashable, Sendable {
        /// The request never reaches the server.
        case dropRequest
        /// The server admits the batch, then the response is lost. The client must retry,
        /// and the retry must not apply anything twice.
        case dropResponse
        /// The request is delivered twice; the client sees the second answer.
        case duplicateRequest
    }

    private let server: CommitLog
    private let identity: ActorID
    public private(set) var isOnline = true
    private var pendingFaults: [Fault] = []
    public private(set) var pushRequestsDelivered = 0

    public init(server: CommitLog, authenticatedAs identity: ActorID) {
        self.server = server
        self.identity = identity
    }

    public func setOnline(_ online: Bool) { isOnline = online }

    /// Queues a fault for the next push. Faults are consumed one per push, in order.
    public func injectOnNextPush(_ fault: Fault) { pendingFaults.append(fault) }

    /// Discards queued faults that haven't fired yet.
    public func clearFaults() { pendingFaults.removeAll() }

    public func push(_ batch: [Mutation]) async throws -> [PushResult] {
        guard isOnline else { throw TransportFailure("offline") }
        let fault = pendingFaults.isEmpty ? nil : pendingFaults.removeFirst()
        switch fault {
        case .dropRequest?:
            throw TransportFailure("request dropped")
        case .dropResponse?:
            _ = await server.push(batch, as: identity)
            pushRequestsDelivered += 1
            throw TransportFailure("response dropped")
        case .duplicateRequest?:
            _ = await server.push(batch, as: identity)
            pushRequestsDelivered += 1
            fallthrough
        case nil:
            pushRequestsDelivered += 1
            return await server.push(batch, as: identity)
        }
    }

    public func pull(after cursor: UInt64, limit: Int) async throws -> PullResponse {
        guard isOnline else { throw TransportFailure("offline") }
        return await server.pull(after: cursor, limit: limit)
    }
}
