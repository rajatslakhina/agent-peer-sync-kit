import Foundation

/// A tool definition in the shape the Claude Messages API takes in `tools`
/// (`name`, `description`, `input_schema`).
public struct ToolDefinition: Hashable, Sendable {
    public let name: String
    public let description: String
    public let inputSchema: JSONValue

    public var json: JSONValue {
        .object(["name": .string(name), "description": .string(description), "input_schema": inputSchema])
    }
}

public enum ToolInputError: Error, Hashable, Sendable {
    case notAnObject
    case unexpectedKey(String)
    case missingKey(String)
    case invalidRecordID
    case noEdits
    case tooManyEdits(limit: Int)
    /// The model named a field that the tool does not offer to this actor.
    case fieldNotOffered(String)
    case wrongValueType(field: String)
    case textTooLong(field: String, limit: Int)
    case deleteNotOffered
}

/// Derives an agent's tool from its server-side policy, and parses what the model sends back.
///
/// The model is only ever offered the fields its actor may write, and the schema says which
/// of them apply immediately and which wait for a human. `decode` is strict (unknown keys,
/// unoffered fields and wrong types are errors), so a tool call that would be refused by
/// the server is usually caught before it reaches the outbox. The server still checks
/// everything again: this is defence in depth, not the authority.
public struct AgentToolSurface: Sendable {
    public static let toolName = "propose_record_edit"

    public let schema: Schema
    public let policy: ActorPolicy

    public init(schema: Schema, policy: ActorPolicy) {
        self.schema = schema
        self.policy = policy
    }

    /// Fields the tool offers, sorted, with their grant.
    public var offeredFields: [(field: FieldName, kind: FieldKind, grant: ActorPolicy.Grant)] {
        schema.fields.keys.sorted().compactMap { field in
            guard let kind = schema.fields[field], let grant = policy.grant(for: field) else { return nil }
            return (field, kind, grant)
        }
    }

    public func definition() -> ToolDefinition {
        var editShapes: [JSONValue] = offeredFields.map { entry in
            let valueKey: String
            let valueSchema: JSONValue
            switch entry.kind {
            case let .text(limit, _):
                valueKey = "value"
                valueSchema = .object(["type": .string("string"), "maxLength": .number(Double(limit))])
            case .flag:
                valueKey = "value"
                valueSchema = .object(["type": .string("boolean")])
            case .counter:
                valueKey = "add"
                valueSchema = .object(["type": .string("integer")])
            }
            return .object([
                "type": .string("object"),
                "additionalProperties": .bool(false),
                "required": .array([.string("field"), .string(valueKey)]),
                "properties": .object([
                    "field": .object(["const": .string(entry.field.rawValue)]),
                    valueKey: valueSchema,
                ]),
            ])
        }
        if policy.mayDelete {
            editShapes.append(.object([
                "type": .string("object"),
                "additionalProperties": .bool(false),
                "required": .array([.string("delete")]),
                "properties": .object(["delete": .object(["const": .bool(true)])]),
            ]))
        }

        let lines = offeredFields.map { entry in
            "- \(entry.field.rawValue): " + (entry.grant == .direct ? "applies immediately" : "held until a person accepts it")
        }
        let description = """
        Edit one record in the shared workspace. Your edit syncs like any other device's: it is \
        checked by the server and may be refused. Fields you may edit:
        \(lines.joined(separator: "\n"))
        If an edit touches any field that is held for review, the whole edit is held.
        """

        let inputSchema: JSONValue = .object([
            "type": .string("object"),
            "additionalProperties": .bool(false),
            "required": .array([.string("record_id"), .string("edits")]),
            "properties": .object([
                "record_id": .object(["type": .string("string"), "pattern": .string("^[a-z0-9._-]{1,64}$")]),
                "edits": .object([
                    "type": .string("array"),
                    "minItems": .number(1),
                    "maxItems": .number(Double(schema.maxOpsPerMutation)),
                    "items": .object(["oneOf": .array(editShapes)]),
                ]),
            ]),
        ])
        return ToolDefinition(name: Self.toolName, description: description, inputSchema: inputSchema)
    }

    /// Parses a `tool_use` input into a record and ops. Never traps on model output.
    public func decode(_ input: JSONValue) throws -> (record: RecordID, ops: [FieldOp]) {
        guard case let .object(top) = input else { throw ToolInputError.notAnObject }
        try Self.requireOnly(["record_id", "edits"], in: top)
        guard case let .string(rawID)? = top["record_id"] else { throw ToolInputError.missingKey("record_id") }
        guard let record = RecordID(rawID) else { throw ToolInputError.invalidRecordID }
        guard case let .array(edits)? = top["edits"] else { throw ToolInputError.missingKey("edits") }
        guard !edits.isEmpty else { throw ToolInputError.noEdits }
        guard edits.count <= schema.maxOpsPerMutation else {
            throw ToolInputError.tooManyEdits(limit: schema.maxOpsPerMutation)
        }

        let offered = Dictionary(uniqueKeysWithValues: offeredFields.map { ($0.field.rawValue, $0) })
        return (record, try edits.map { edit in
            guard case let .object(body) = edit else { throw ToolInputError.notAnObject }
            if body["delete"] != nil {
                guard policy.mayDelete else { throw ToolInputError.deleteNotOffered }
                try Self.requireOnly(["delete"], in: body)
                guard body["delete"] == .bool(true) else { throw ToolInputError.wrongValueType(field: "delete") }
                return .delete
            }
            guard case let .string(rawField)? = body["field"] else { throw ToolInputError.missingKey("field") }
            guard let entry = offered[rawField] else { throw ToolInputError.fieldNotOffered(rawField) }
            switch entry.kind {
            case let .text(limit, _):
                try Self.requireOnly(["field", "value"], in: body)
                guard case let .string(text)? = body["value"] else { throw ToolInputError.wrongValueType(field: rawField) }
                guard text.unicodeScalars.count <= limit else { throw ToolInputError.textTooLong(field: rawField, limit: limit) }
                return .set(entry.field, .text(text))
            case .flag:
                try Self.requireOnly(["field", "value"], in: body)
                guard case let .bool(flag)? = body["value"] else { throw ToolInputError.wrongValueType(field: rawField) }
                return .set(entry.field, .flag(flag))
            case .counter:
                try Self.requireOnly(["field", "add"], in: body)
                // `Int64(exactly:)` returns nil for NaN, infinities, fractions and anything
                // out of range, where `Int64(_:)` would trap.
                guard case let .number(number)? = body["add"], let delta = Int64(exactly: number) else {
                    throw ToolInputError.wrongValueType(field: rawField)
                }
                return .add(entry.field, delta)
            }
        })
    }

    public enum CallResult: Hashable, Sendable {
        /// Parsed and queued in the agent's outbox; the server decides at the next sync.
        case queued(MutationID)
        /// Refused before reaching the outbox; nothing was queued.
        case refused(ToolInputError)
        /// The replica could not allocate an idempotency key.
        case replicaFailed(ReplicaError)
    }

    /// Parses one `tool_use` input and, if it is valid, writes it to the agent's replica.
    /// The agent gets no other write path: the result goes through the same outbox,
    /// idempotency keys, admission rules and log as an edit made on a phone.
    public func submit(_ input: JSONValue, to replica: Replica) async -> CallResult {
        let parsed: (record: RecordID, ops: [FieldOp])
        do {
            parsed = try decode(input)
        } catch let error as ToolInputError {
            return .refused(error)
        } catch {
            return .refused(.notAnObject) // unreachable: decode only throws ToolInputError
        }
        do {
            return .queued(try await replica.write(parsed.record, parsed.ops))
        } catch let error as ReplicaError {
            return .replicaFailed(error)
        } catch {
            return .replicaFailed(.sequenceExhausted) // unreachable: write only throws ReplicaError
        }
    }

    private static func requireOnly(_ allowed: Set<String>, in object: [String: JSONValue]) throws {
        if let extra = object.keys.sorted().first(where: { !allowed.contains($0) }) {
            throw ToolInputError.unexpectedKey(extra)
        }
    }
}
