import AgentPeerSync
import Foundation

/// Stands in for a model call. A real agent would send `AgentToolSurface.definition()` in
/// `tools` and get these inputs back in `tool_use` blocks; the scripted calls include the
/// mistakes models make, so the demo shows what happens to them.
public enum ScriptedNotesAgent {
    public struct Call: Hashable, Sendable {
        public let label: String
        public let input: JSONValue
    }

    public static func calls() -> [Call] {
        [
            Call(label: "Summarise item-1 (its own field: applies directly)", input: edit("item-1", [
                ["field": .string("summary"), "value": .string("Agreed: ship offline mode behind a flag; Asha owns rollout.")],
            ])),
            Call(label: "Retitle item-3 (human text: proposed)", input: edit("item-3", [
                ["field": .string("title"), "value": .string("Write the rollout runbook")],
            ])),
            Call(label: "Retitle item-2 (human text: proposed)", input: edit("item-2", [
                ["field": .string("title"), "value": .string("Ship offline mode to 10% of users")],
            ])),
            Call(label: "Tick off item-2 (not offered to the agent)", input: edit("item-2", [
                ["field": .string("done"), "value": .bool(true)],
            ])),
            Call(label: "Summary with an extra key the schema forbids", input: edit("item-3", [
                ["field": .string("summary"), "value": .string("Runbook needed."), "confidence": .number(0.9)],
            ])),
        ]
    }

    private static func edit(_ record: String, _ edits: [[String: JSONValue]]) -> JSONValue {
        .object(["record_id": .string(record), "edits": .array(edits.map { .object($0) })])
    }
}

/// A scripted day: two people edit offline, an agent proposes changes, a response is lost,
/// a reviewer settles the proposals, and everyone converges. Shared by the demo app and an
/// integration test, so the screenshots and the test exercise the same code path.
public struct ScenarioStep: Sendable {
    public let title: String
    public let run: @Sendable (Workspace) async -> [String]
}

public enum OfflineDayScenario {
    /// The sample agent's tool surface, built from `SampleWorkspace.agentPolicy`.
    public static var sampleSurface: AgentToolSurface {
        AgentToolSurface(schema: SampleWorkspace.schema, policy: SampleWorkspace.agentPolicy)
    }

    public static var steps: [ScenarioStep] { steps(agentSurface: sampleSurface) }

    /// The scripted day. Step 3 submits the agent's tool calls through `agentSurface`, so a
    /// host that builds its own surface (as the demo app does) is what decides them.
    public static func steps(agentSurface surface: AgentToolSurface) -> [ScenarioStep] {
        [
            ScenarioStep(title: "Asha creates three action items and everyone syncs") { ws in
                var lines: [String] = []
                lines += await write(ws.asha, Names.item1, [.set(Names.title, .text("Ship offline mode")), .add(Names.votes, 0)], "Asha")
                lines += await write(ws.asha, Names.item2, [.set(Names.title, .text("Ship offline mode to beta")), .set(Names.done, .flag(false))], "Asha")
                lines += await write(ws.asha, Names.item3, [.set(Names.title, .text("Runbook"))], "Asha")
                for (name, replica) in [("Asha", ws.asha), ("Ben", ws.ben), ("Agent", ws.agent)] {
                    lines.append(await sync(replica, name))
                }
                return lines
            },
            ScenarioStep(title: "Asha and Ben go offline and both edit item-2 and vote on item-1") { ws in
                await ws.ashaLink.setOnline(false)
                await ws.benLink.setOnline(false)
                var lines = ["Asha and Ben are offline."]
                lines += await write(ws.asha, Names.item2, [.set(Names.title, .text("Ship offline mode in 2.4"))], "Asha")
                lines += await write(ws.ben, Names.item2, [.set(Names.title, .text("Ship offline mode (beta)"))], "Ben")
                lines += await write(ws.asha, Names.item1, [.add(Names.votes, 1)], "Asha")
                lines += await write(ws.ben, Names.item1, [.add(Names.votes, 1)], "Ben")
                return lines
            },
            ScenarioStep(title: "The agent turns the meeting transcript into tool calls") { ws in
                var lines: [String] = []
                for call in ScriptedNotesAgent.calls() {
                    switch await surface.submit(call.input, to: ws.agent) {
                    case let .queued(id): lines.append("Agent: \(call.label) → queued as \(id)")
                    case let .refused(error): lines.append("Agent: \(call.label) → refused locally: \(Describe.toolError(error))")
                    case let .replicaFailed(error): lines.append("Agent: \(call.label) → replica error \(error)")
                    }
                }
                lines.append(await sync(ws.agent, "Agent"))
                return lines
            },
            ScenarioStep(title: "Asha reconnects; the response to her push is lost, so she retries") { ws in
                await ws.ashaLink.setOnline(true)
                await ws.ashaLink.injectOnNextPush(.dropResponse)
                var lines: [String] = []
                lines.append(await sync(ws.asha, "Asha (1st try)"))
                lines.append(await sync(ws.asha, "Asha (retry)"))
                return lines
            },
            ScenarioStep(title: "Ben reconnects: his title is refused as stale, his vote merges") { ws in
                await ws.benLink.setOnline(true)
                return [await sync(ws.ben, "Ben")] + (await ws.ben.view()).rejections.map {
                    "Ben's \($0.id) was refused: \(Describe.reason($0.reason))"
                }
            },
            ScenarioStep(title: "Asha accepts both agent proposals; the server re-checks each one") { ws in
                var lines: [String] = []
                for proposal in (await ws.asha.view()).confirmed.openProposals {
                    lines += await review(ws.asha, proposal.seq, .accept, "Asha")
                }
                lines.append(await sync(ws.asha, "Asha"))
                for entry in await ws.server.retainedEntries() {
                    if case let .resolved(proposal, resolution, _) = entry.event {
                        lines.append("Proposal @\(proposal): \(Describe.resolution(resolution))")
                    }
                }
                return lines
            },
            ScenarioStep(title: "Everyone pulls; every replica matches the server") { ws in
                var lines: [String] = []
                for (name, replica) in [("Asha", ws.asha), ("Ben", ws.ben), ("Agent", ws.agent)] {
                    lines.append(await sync(replica, name))
                }
                let check = await ws.convergence()
                lines.append(check.converged
                    ? "Converged: all replicas equal the server at v\(check.serverHead)."
                    : "NOT converged at v\(check.serverHead).")
                return lines
            },
        ]
    }

    private static func write(_ replica: Replica, _ record: RecordID, _ ops: [FieldOp], _ name: String) async -> [String] {
        do {
            let id = try await replica.write(record, ops)
            return ["\(name) edits \(record) offline-first → \(id)"]
        } catch {
            return ["\(name) could not queue an edit: \(error)"]
        }
    }

    private static func review(_ replica: Replica, _ proposal: UInt64, _ decision: ReviewDecision, _ name: String) async -> [String] {
        do {
            let id = try await replica.review(proposal, decision)
            return ["\(name) \(decision.rawValue)s proposal @\(proposal) → \(id)"]
        } catch {
            return ["\(name) could not queue a review: \(error)"]
        }
    }

    private static func sync(_ replica: Replica, _ name: String) async -> String {
        do {
            let report = try await replica.sync()
            var parts = ["\(name) synced:"]
            if report.committed > 0 { parts.append("\(report.committed) committed") }
            if report.proposed > 0 { parts.append("\(report.proposed) proposed") }
            if report.rejected > 0 { parts.append("\(report.rejected) refused") }
            parts.append("pulled \(report.pulledEntries), now at v\(await replica.cursor)")
            return parts.joined(separator: " ")
        } catch let failure as TransportFailure {
            return "\(name) sync failed: \(failure.reason) (outbox kept)"
        } catch {
            return "\(name) sync failed: \(error) (outbox kept)"
        }
    }
}

/// Human-readable text for outcomes, used by the scenario log and the demo UI.
public enum Describe {
    public static func reason(_ reason: RejectionReason) -> String {
        switch reason {
        case .unknownActor: return "unknown or revoked actor"
        case .actorMismatch: return "author is not the authenticated caller"
        case let .unknownField(field): return "unknown field \(field)"
        case let .typeMismatch(field): return "wrong kind of change for \(field)"
        case let .textTooLong(field, limit): return "\(field) longer than \(limit)"
        case let .notPermitted(field): return "not permitted to write \(field)"
        case .deleteNotPermitted: return "not permitted to delete"
        case .deleteMustBeAlone: return "a delete must be the only change"
        case .noSuchRecord: return "no such record"
        case .createNotPermitted: return "not permitted to create records"
        case .emptyMutation: return "empty edit"
        case let .tooManyOps(limit): return "more than \(limit) changes"
        case .recordDeleted: return "record was deleted"
        case let .staleWrite(field, version): return "\(field) was changed by someone else at v\(version)"
        case .reviewNotPermitted: return "not permitted to review"
        case .selfReview: return "can't review your own proposal"
        case let .noSuchProposal(seq): return "no open proposal @\(seq)"
        case let .proposalQueueFull(limit): return "\(limit) proposals already waiting"
        case .idempotencyConflict: return "idempotency key reused with a different edit"
        case .retryExpired: return "retry is older than the replay window"
        case let .baseVersionAhead(head): return "edit claims a version past the head (v\(head))"
        case let .batchTooLarge(limit): return "more than \(limit) edits in one push"
        case .logFull: return "log sequence exhausted"
        }
    }

    public static func resolution(_ resolution: Resolution) -> String {
        switch resolution {
        case .applied: return "accepted and applied"
        case .rejected: return "rejected"
        case let .superseded(reason): return "accepted but no longer applies (\(Describe.reason(reason)))"
        }
    }

    public static func toolError(_ error: ToolInputError) -> String {
        switch error {
        case .notAnObject: return "input is not an object"
        case let .unexpectedKey(key): return "unexpected key \"\(key)\""
        case let .missingKey(key): return "missing \"\(key)\""
        case .invalidRecordID: return "invalid record id"
        case .noEdits: return "no edits"
        case let .tooManyEdits(limit): return "more than \(limit) edits"
        case let .fieldNotOffered(field): return "field \"\(field)\" is not offered to this agent"
        case let .wrongValueType(field): return "wrong value type for \(field)"
        case let .textTooLong(field, limit): return "\(field) longer than \(limit)"
        case .deleteNotOffered: return "delete is not offered to this agent"
        }
    }
}
