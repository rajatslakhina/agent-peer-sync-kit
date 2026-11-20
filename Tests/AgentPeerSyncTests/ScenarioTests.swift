@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

/// Runs the exact script the demo app plays and checks the outcome of every beat.
final class ScenarioTests: XCTestCase {
    func testOfflineDayEndsConvergedWithEveryConflictSettledAsDesigned() async throws {
        let ws = Workspace()
        var log: [String] = []
        for step in OfflineDayScenario.steps { log += await step.run(ws) }

        let state = await ws.server.snapshot()
        let item1 = state.records[Names.item1]
        let item2 = state.records[Names.item2]
        let item3 = state.records[Names.item3]

        // Both offline votes count, once each, despite Asha's lost response.
        XCTAssertEqual(item1?[Names.votes], .count(2))
        // The agent's own field applied directly.
        XCTAssertEqual(item1?.writes[Names.summary]?.actor, Names.agent)
        // Asha's retitle reached the server first; Ben's stale retitle was refused.
        XCTAssertEqual(item2?[Names.title], .text("Ship offline mode in 2.4"))
        let benRefusals = await ws.ben.view().rejections.map(\.reason)
        XCTAssertEqual(benRefusals, [.staleWrite(Names.title, currentVersion: 7)])
        // The agent's item-3 proposal was accepted and applied, attributed to the agent.
        XCTAssertEqual(item3?[Names.title], .text("Write the rollout runbook"))
        XCTAssertEqual(item3?.writes[Names.title]?.actor, Names.agent)
        // Its item-2 proposal was accepted too, but superseded by Asha's newer title.
        let resolutions = await ws.server.retainedEntries().compactMap { entry -> Resolution? in
            if case let .resolved(_, resolution, _) = entry.event { return resolution }
            return nil
        }
        XCTAssertEqual(resolutions, [.applied, .superseded(.staleWrite(Names.title, currentVersion: 7))])
        XCTAssertTrue(state.openProposals.isEmpty)
        // Two of the five tool calls never reached the outbox.
        XCTAssertEqual(log.filter { $0.contains("refused locally") }.count, 2)
        // Every replica ends equal to the server.
        let check = await ws.convergence()
        XCTAssertTrue(check.converged, "\(check.rows)")
        XCTAssertEqual(log.last, "Converged: all replicas equal the server at v\(check.serverHead).")
    }

    func testEveryStepCanBeReplayedIntoAFreshWorkspaceStepByStep() async {
        // The demo runs steps one tap at a time; replaying a prefix must never crash or hang.
        let steps = OfflineDayScenario.steps
        let expectedHeads: [UInt64] = [0, 3, 3, 6, 8, 9, 11, 11]
        XCTAssertEqual(expectedHeads.count, steps.count + 1)
        for prefix in 0...steps.count where expectedHeads.indices.contains(prefix) {
            let ws = Workspace()
            for step in steps.prefix(prefix) { _ = await step.run(ws) }
            let head = await ws.server.head
            XCTAssertEqual(head, expectedHeads[prefix], "after \(prefix) steps")
        }
    }
}
