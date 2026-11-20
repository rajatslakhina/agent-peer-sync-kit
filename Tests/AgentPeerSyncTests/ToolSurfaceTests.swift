@testable import AgentPeerSync
import AgentPeerSyncTesting
import XCTest

final class ToolSurfaceTests: XCTestCase {
    private let agentSurface = AgentToolSurface(schema: SampleWorkspace.schema, policy: SampleWorkspace.agentPolicy)

    private func editFields(in definition: ToolDefinition?) -> [String] {
        guard let definition, case let .object(schema) = definition.inputSchema,
              case let .object(properties)? = schema["properties"],
              case let .object(edits)? = properties["edits"],
              case let .object(items)? = edits["items"],
              case let .array(shapes)? = items["oneOf"] else { return [] }
        return shapes.compactMap { shape in
            guard case let .object(body) = shape, case let .object(props)? = body["properties"] else { return nil }
            if case let .object(field)? = props["field"], case let .string(name)? = field["const"] { return name }
            return props["delete"] != nil ? "<delete>" : nil
        }
    }

    func testToolOffersOnlyTheFieldsTheAgentMayWrite() {
        guard let definition = agentSurface.definition() else { return XCTFail("the agent is offered fields") }
        XCTAssertEqual(definition.name, "propose_record_edit")
        XCTAssertEqual(editFields(in: definition), ["notes", "summary", "title"])
        XCTAssertTrue(definition.description.contains("- summary: applies immediately"))
        XCTAssertTrue(definition.description.contains("- title: held until a person accepts it"))
        XCTAssertFalse(definition.description.contains("votes"))
    }

    func testDeleteIsOfferedOnlyWithTheGrant() {
        let person = AgentToolSurface(schema: SampleWorkspace.schema, policy: SampleWorkspace.personPolicy)
        XCTAssertEqual(editFields(in: person.definition()), ["done", "notes", "summary", "title", "votes", "<delete>"])
        let input = JSONValue.parse(#"{"record_id":"item-1","edits":[{"delete":true}]}"#) ?? .null
        XCTAssertEqual(try person.decode(input).ops, [.delete])
        XCTAssertThrowsError(try agentSurface.decode(input)) { XCTAssertEqual($0 as? ToolInputError, .deleteNotOffered) }
    }

    func testDefinitionSerialisesWithSortedKeysAndRoundTrips() {
        guard let json = agentSurface.definition()?.json else { return XCTFail("definition") }
        let text = json.jsonString()
        XCTAssertTrue(text.hasPrefix(#"{"description":"#), "keys are sorted, so description comes first")
        XCTAssertTrue(text.contains(#""additionalProperties":false"#))
        XCTAssertTrue(text.contains(#""maxLength":120"#))
        XCTAssertEqual(JSONValue.parse(text), json, "round-trips")
    }

    func testAPolicyThatOffersNothingHasNoTool() {
        let nothing = ActorPolicy(kind: .agent, directFields: [], mayCreate: false, mayDelete: false, mayReview: false)
        XCTAssertNil(AgentToolSurface(schema: SampleWorkspace.schema, policy: nothing).definition())
        let deleteOnly = ActorPolicy(kind: .agent, directFields: [], mayCreate: false, mayDelete: true, mayReview: false)
        XCTAssertEqual(editFields(in: AgentToolSurface(schema: SampleWorkspace.schema, policy: deleteOnly).definition()), ["<delete>"])
    }

    func testDecodesAValidCall() throws {
        let input = JSONValue.parse(#"{"record_id":"item-3","edits":[{"field":"summary","value":"ok"},{"field":"title","value":"New"}]}"#) ?? .null
        let decoded = try agentSurface.decode(input)
        XCTAssertEqual(decoded.record, Names.item3)
        XCTAssertEqual(decoded.ops, [.set(Names.summary, .text("ok")), .set(Names.title, .text("New"))])
    }

    func testRejectsWhatAModelGetsWrong() {
        let cases: [(String, ToolInputError)] = [
            (#"[]"#, .notAnObject),
            (#"{"record_id":"item-1","edits":[],"reason":"x"}"#, .unexpectedKey("reason")),
            (#"{"edits":[]}"#, .missingKey("record_id")),
            (#"{"record_id":"Item 1","edits":[]}"#, .invalidRecordID),
            (#"{"record_id":"item-1","edits":[]}"#, .noEdits),
            (#"{"record_id":"item-1","edits":[{"field":"done","value":true}]}"#, .fieldNotOffered("done")),
            (#"{"record_id":"item-1","edits":[{"field":"summary","value":"s","confidence":0.9}]}"#, .unexpectedKey("confidence")),
            (#"{"record_id":"item-1","edits":[{"field":"summary","value":3}]}"#, .wrongValueType(field: "summary")),
            (#"{"record_id":"item-1","edits":[{"value":"s"}]}"#, .missingKey("field")),
        ]
        for (text, expected) in cases {
            let input = JSONValue.parse(text) ?? .null
            XCTAssertThrowsError(try agentSurface.decode(input), text) { error in
                XCTAssertEqual(error as? ToolInputError, expected, text)
            }
        }
    }

    func testTextLimitAndEditCountAreEnforced() {
        let long = String(repeating: "x", count: 501)
        let tooLong = JSONValue.object(["record_id": .string("item-1"), "edits": .array([
            .object(["field": .string("summary"), "value": .string(long)]),
        ])])
        XCTAssertThrowsError(try agentSurface.decode(tooLong)) {
            XCTAssertEqual($0 as? ToolInputError, .textTooLong(field: "summary", limit: 500))
        }
        let edit = JSONValue.object(["field": .string("summary"), "value": .string("s")])
        let many = JSONValue.object(["record_id": .string("item-1"),
                                     "edits": .array(Array(repeating: edit, count: SampleWorkspace.schema.maxOpsPerMutation + 1))])
        XCTAssertThrowsError(try agentSurface.decode(many)) {
            XCTAssertEqual($0 as? ToolInputError, .tooManyEdits(limit: SampleWorkspace.schema.maxOpsPerMutation))
        }
    }

    func testCounterDeltasThatDontFitInt64AreRejectedNotTrapped() throws {
        let person = AgentToolSurface(schema: SampleWorkspace.schema, policy: SampleWorkspace.personPolicy)
        // 9007199254740993 isn't representable as a Double; it arrives as ...992, past 2^53 - 1.
        let rounded = JSONValue.parse(#"{"record_id":"item-1","edits":[{"field":"votes","add":9007199254740993}]}"#) ?? .null
        XCTAssertThrowsError(try person.decode(rounded)) { XCTAssertEqual($0 as? ToolInputError, .wrongValueType(field: "votes")) }
        let largestExact = JSONValue.object(["record_id": .string("item-1"), "edits": .array([
            .object(["field": .string("votes"), "add": .number(-9_007_199_254_740_991)]),
        ])])
        XCTAssertEqual(try person.decode(largestExact).ops, [.add(Names.votes, -AgentToolSurface.maxCounterDelta)])
        for bad in [1e300, -1e300, 0.5, 9.3e18, .infinity, .nan] {
            let input = JSONValue.object(["record_id": .string("item-1"), "edits": .array([
                .object(["field": .string("votes"), "add": .number(bad)]),
            ])])
            XCTAssertThrowsError(try person.decode(input), "\(bad)") {
                XCTAssertEqual($0 as? ToolInputError, .wrongValueType(field: "votes"))
            }
        }
        let ok = JSONValue.object(["record_id": .string("item-1"), "edits": .array([
            .object(["field": .string("votes"), "add": .number(-3)]),
        ])])
        XCTAssertEqual(try person.decode(ok).ops, [.add(Names.votes, -3)])
    }

    func testJSONKeepsBooleansAndNumbersApart() {
        XCTAssertEqual(JSONValue.parse("[true, 1, 0, false, null]"),
                       .array([.bool(true), .number(1), .number(0), .bool(false), .null]))
        XCTAssertNil(JSONValue.parse("{not json"))
    }

    /// Everything the tool offers is something the server admits: the surface and the server
    /// policy can't drift apart for this schema.
    func testEveryOfferedEditIsAdmittedByTheServer() async throws {
        let ws = Workspace()
        try await ws.asha.write(Names.item1, [.set(Names.title, .text("T"))])
        try await ws.asha.sync()
        try await ws.agent.sync()
        var calls = 0
        for entry in agentSurface.offeredFields {
            let value: JSONValue
            switch entry.kind {
            case .text: value = .string("x")
            case .flag: value = .bool(true)
            case .counter: value = .number(1)
            }
            let key = entry.kind == .counter ? "add" : "value"
            let input = JSONValue.object(["record_id": .string("item-1"), "edits": .array([
                .object(["field": .string(entry.field.rawValue), key: value]),
            ])])
            guard case .queued = await agentSurface.submit(input, to: ws.agent) else { return XCTFail("\(entry.field) refused") }
            calls += 1
        }
        XCTAssertEqual(calls, 3)
        let report = try await ws.agent.sync()
        XCTAssertEqual(report.rejected, 0)
        XCTAssertEqual(report.committed + report.proposed, calls)
        XCTAssertEqual(report.proposed, 2, "title and notes are held for review")
    }
}
