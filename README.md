# AgentPeerSync

**Your AI agent is just another offline device.** A local-first sync engine for an iOS app where a Claude-style agent writes through the same mutation log as the user's phones: its own replica, its own cursor, its own idempotency keys, and a server-side write grant that decides which of its edits apply at once and which wait for a person.

Demo app: (added after the companion repo is pushed)

[![CI](https://github.com/rajatslakhina/agent-peer-sync-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/rajatslakhina/agent-peer-sync-kit/actions/workflows/ci.yml)
Swift 6 · iOS 17+ / macOS 14+ · Linux · Foundation only · no dependencies

## The problem

Agents now write into app data: a hosted or self-hosted Managed Agent working through a task list, a meeting agent filing notes and action items. The quick way to wire one in is a privileged API route that writes straight to the database. That route skips everything the app's sync design depends on:

- **Ordering and idempotency.** Phones retry; so do agents. Without a per-writer sequence and a replayable outcome, a lost response turns into a second application.
- **Conflict rules.** A phone that was offline for an hour is held to "you edited a stale title". A back-door agent write isn't, so it silently overwrites a person's newer text.
- **Authorization at the right grain.** "The agent may write" is not a policy. Which fields, directly or only as a proposal, and can it create, delete or approve?
- **One audit trail.** If agent writes don't go through the log, the log no longer describes how the data got here.

AgentPeerSync takes the opposite position: the agent gets **no back door**. It is a peer with a replica like any phone, and its tool calls become ordinary mutations in that replica's outbox.

## How it works

```
 Asha's phone          Ben's phone           Notes agent
 Replica               Replica               Replica  ◀── AgentToolSurface (tool schema derived from its grant,
 outbox · cursor       outbox · cursor       outbox · cursor        strict decode of tool_use input)
      │  push / pull        │                     │
      └─────────────────────┴──────────┬──────────┘
                                       ▼
                               CommitLog (server-authoritative)
   1. idempotency: (actor, clientSeq) — next seq is admitted, a repeat replays its stored outcome,
      a gap is refused, a reused key with a different body is refused
   2. admission: schema + the caller's server-side ActorPolicy + current state
        → commit  (applied now)      → propose (recorded, changes nothing until a person accepts)
        → reject  (reason returned; the sequence number is still consumed)
   3. append to the log; every replica replays the same entries with the same reducer
```

| Type | Role |
|---|---|
| `CommitLog` (actor) | The authority. `push(_:as:)` admits a batch (up to `maxPushBatch`) for an authenticated caller; `pull(after:limit:)` pages the log or returns a snapshot. Past `maxRetainedEntries` it folds all but the newest `maxRetainedEntries / 2` entries into a snapshot. |
| `Replica` (actor) | One per device or agent. `write` / `review` queue a mutation with a fresh idempotency key; `sync()` pushes, then pulls and rebases. `view()` returns confirmed state, the optimistic `visible` state, the outbox and refusals. `persistentState()` / `init(restoring:)` survive a relaunch, and an optional `persist` handler saves the state before every push (write-ahead). |
| `Admission` | The server's rules as a pure function: field kinds, text limits, grants, stale-write detection, delete/create rights, proposal backpressure. |
| `ActorPolicy` | Per actor, held by the server: `directFields`, `reviewedFields`, `mayCreate`, `mayDelete`, `mayReview`, `maxOpenProposals`. |
| `Snapshot` | The shared state at a version: records, open proposals, and per-actor `appliedThrough`. Built by one reducer on the server and on every replica. |
| `AgentToolSurface` | Turns an agent's policy into a Messages API tool definition (`name`, `description`, `input_schema`) and parses `tool_use` input strictly into ops. |
| `SyncTransport` | The seam to the network. The transport carries the credential, so the replica can't choose who it is. |
| `AgentPeerSyncTesting` | `LocalTransport` with fault injection (`dropRequest`, `dropResponse`, `duplicateRequest`), the sample workspace, and the scripted day the demo plays. For tests and demos, not production. |

### Using it

End to end, with the in-process transport from `AgentPeerSyncTesting` (swap in your own `SyncTransport` for a real server). The first block binds everything to the ready-made sample in that module, so this runs as pasted (inside an `async throws` context):

```swift
import AgentPeerSync
import AgentPeerSyncTesting

let schema = SampleWorkspace.schema
let (personPolicy, agentPolicy) = (SampleWorkspace.personPolicy, SampleWorkspace.agentPolicy)
let (phone, agent) = (Names.asha, Names.agent)
let (itemID, title, votes) = (Names.item1, Names.title, Names.votes)

let log = CommitLog(schema: schema, policies: [phone: personPolicy, agent: agentPolicy])
let phoneReplica = Replica(actor: phone, transport: LocalTransport(server: log, authenticatedAs: phone),
                           persist: { state in /* store state durably, e.g. JSON to a file */ _ = state })
let agentReplica = Replica(actor: agent, transport: LocalTransport(server: log, authenticatedAs: agent))

try await phoneReplica.write(itemID, [.set(title, .text("Ship offline mode")), .add(votes, 1)])
try await phoneReplica.sync()                         // push the outbox, pull the log, rebase
print(await phoneReplica.view().visible.liveRecords)  // confirmed state + this replica's pending edits

// The agent: send this in `tools` on a Messages API request…
let surface = AgentToolSurface(schema: schema, policy: agentPolicy)
print(surface.definition()?.json.jsonString() ?? "no tool")
// …and route each `tool_use` input (a JSONValue) back through the same pipe as a phone edit.
let toolUseInput = JSONValue.parse(#"{"record_id":"item-1","edits":[{"field":"summary","value":"Ship behind a flag"}]}"#) ?? .null
switch await surface.submit(toolUseInput, to: agentReplica) {
case .queued: try await agentReplica.sync()           // the server decides: commit, propose or reject
case .refused(let error): print(error)                // never reached the outbox; tell the model why
case .replicaFailed(let error): print(error)
}
```

A production `SyncTransport` (say, over `URLSession`) has three duties: authenticate the caller and let the *server* map the credential to an `ActorID` (the replica never asserts who it is); send `push` batches and `pull(after:limit:)` cursors as-is; and throw on any transport failure without guessing an outcome, because the replica retries with the same idempotency keys.

## Design decisions and trade-offs

**Conflict resolution happens once, on the server, at admission.** Replicas never re-decide anything; they replay admitted entries with the same reducer (`Snapshot.apply`). Convergence is then a property of the log, not of each client's merge code. *Rejected:* client-side CRDT merge for everything. It's the right tool for peer-to-peer or rich text, but a CRDT can't express "this agent may only propose title changes" or "a person must approve", and authorization is the point here.

**Per-field conflict policy instead of one rule.** `rejectStale` for fields where a silent overwrite destroys someone's work (titles), `lastWriterWins` for cheap fields (notes, flags), and counters that only change by `add`, so concurrent offline votes all count. "Last" means last admitted by the server, not a wall clock: device clocks are not trusted. An actor's own earlier write never makes its next write stale.

**Field-level trust tiers for the agent.** The sample agent writes its own `summary` field directly, may only *propose* changes to `title` and `notes`, and can't create, delete, vote, tick items off or review. A mutation that touches any reviewed field becomes a proposal as a whole (mutations are atomic). Proposals are bounded per author (`maxOpenProposals`), which is backpressure on an agent that floods the queue.

**Acceptance is re-checked against the state now.** When a person accepts, the server re-runs admission against the *current* state and the author's *current* policy. If a person edited a guarded field while the proposal waited, the record was deleted, or the agent's grant was revoked, the proposal closes as `.superseded(reason)` instead of overwriting newer work. *Rejected:* applying the proposal as it was approved in the UI; the UI's copy is exactly the stale view this prevents.

**Review decisions are mutations.** Accept/reject travels through the same outbox, keys and log, so a reviewer can approve offline and the decision is ordered and deduplicated like any edit. No actor can settle its own proposal.

**Idempotency is a per-writer sequence, and rejections consume it.** The server keeps the last `outcomeRetention` outcomes per actor (default 1,024) and replays them for retries; older retries get `.retryExpired`, never a second application. Each kept outcome is stored with a 16-byte fingerprint of the body (two 64-bit FNV-1a hashes of its sorted-key JSON encoding), never the body itself, so a caller can't grow the window's memory by sending huge refused edits. A retry with the same key and a different body is refused as `.idempotencyConflict`. Because a refusal still consumes its number, a client never has to resubmit a refused edit to make progress. *Rejected:* random UUID keys with an unbounded seen-set — a contiguous sequence also gives per-writer ordering and a bounded window for free.

**An edit keeps its key for life, and keys are saved before they're sent.** A replica gives an edit its idempotency key when it is written and never changes it, and the server applies a key at most once, so no edit can be applied twice, whatever crashes, retries or relaunches happen. The `persist` handler is called with the replica's state, and must have stored it, before every push, so a key never reaches the network unsaved; after a crash the replica resends the same keys and gets the stored outcomes back. *Rejected:* re-keying an edit after a relaunch when its key looks taken. Versions 2.0.1–2.0.4 of this library did that, and successive review rounds kept finding sequences (a crash between re-keying and saving, a sync that joins another, a restore from an older save) where one edit ended up under two keys and applied twice. A key that never changes rules that out by construction; the cost is that restoring an older save than the last one stored can lose edits (see limits).

**The optimistic overlay is honest about what it knows.** `visible` is confirmed state plus (a) edits the server committed at a version the replica hasn't pulled yet, so an acknowledged edit doesn't flicker out, and (b) unanswered edits — except ones the log already contains (`appliedThrough`), so an edit whose response was lost isn't counted twice after a pull. A failed push doesn't stop the pull, so pending edits still rebase onto fresh state during a write outage.

**The agent's tool comes from its grant.** `AgentToolSurface` only offers the fields the policy allows and says in the description which apply immediately (a policy that offers nothing gets no tool, rather than an invalid empty schema). `decode` refuses unknown keys, unoffered fields, wrong types, fractions and non-finite numbers, and counter deltas beyond ±(2^53 − 1), the range in which a JSON number arrives as an exact integer; conversions go through `Int64(exactly:)`, never a trapping initializer. This is defence in depth: the server checks everything again.

**Bounded where a caller controls the size.** Identifiers are 1–64 ASCII characters; text limits are counted in Unicode scalars (the unit JSON Schema's `maxLength` uses) and clamped; ops per mutation, mutations admitted per push (500; the rest of a larger batch is answered `.batchTooLarge` without being read), open proposals per author, the replay window (count and, via fingerprints, bytes), the retained log, pull pages, the replica's pending edits (10,000) and its refusal history (50) all have limits. Counters saturate at `Int64.min`/`.max` instead of trapping. The shared state itself grows with the data (see limits).

**Compaction keeps a tail.** When the log passes `maxRetainedEntries` (default 10,000), everything but the newest `maxRetainedEntries / 2` entries is folded into the base snapshot, so a replica that is a little behind still gets entries; only one that is behind the new base gets a full snapshot. A page that doesn't continue from the replica's cursor is never applied past the gap, and the sync report says so (`pullStalled`, also set when a page makes no progress while the server reports more, or the page budget runs out).

## What's tested

80 XCTest tests, run on macOS and Linux in CI, plus a **mutation check**: `scripts/mutation-check.sh` re-introduces 42 specific bugs, one at a time (skip the idempotent replay, apply an accepted proposal without re-checking it, let a stale write through, offer the agent every field, drop the outbox overlay, stop coalescing concurrent syncs, apply an entry past a gap, make the convergence check always pass, …) and fails CI if any of them still passes the suite.

- **Admission** (19): every rule above, including own-write-isn't-stale, scalar-counted text limits, proposal backpressure, and decoded policies and schemas being clamped like constructed ones.
- **Commit log** (18): replay, key reuse, a stable fixed-size fingerprint (checked against a known value, and the stored outcome's layout checked to hold no body), gaps, sequence consumption, batch cut-off, spoofed authors, configuration clamped even when set after init, revocation, replay-window expiry, review permissions, re-check on accept, paging, compaction that keeps a tail.
- **Replica** (17) and **persistence** (11): overlay, no-flicker, rebase-on-pull during a write outage, lost and duplicated responses (also right after a relaunch), edits cut from an oversized batch, coalesced syncs (and a joiner whose edit missed the running push getting its own round), stalled pulls (gap, no progress, page budget), relaunch from saved state, the persist handler saving before every push (and a failed save stopping the push), a crash before the host's own save, a stale save losing an edit but never applying one twice, saves from 2.0.3/2.0.4 still decoding, key bump after a pull, bounded outbox, gap refusal and reporting.
- **Agent tool** (10): offered fields match the grant, no tool for an empty grant, strict decoding, the 2^53 and `Int64` edges, and every offered edit is admitted by the server.
- **Convergence** (3): 36 seeded random schedules of offline edits, reviews, connectivity changes, transport faults, concurrent syncs and app crashes (each replica saves before every push through its persist handler and its app saves after most actions; a crash relaunches it from the latest save or, now and then, from an older one) across two phones and an agent (some with a tiny replay window and aggressive compaction). Each must end with every replica equal to the server, a fresh replica paging one entry at a time equal to the server, and (in the runs without compaction, where the whole log is still there to inspect) no mutation in the log twice, no logical edit in the log twice (every generated edit except deletes carries a unique text value, so one edit landing twice under any keys would show), and each actor's mutations in its own order. A companion test feeds a deliberately broken server to one replica and asserts that the same convergence check reports it (`converged == false`, with that replica's row marked).
- **Scenario** (2): the exact script the demo app plays, checked beat by beat.

## Honest limits

- **The server here is a reference implementation in memory.** `CommitLog` shows the admission and replay rules; a production server would run the same rules inside a database transaction and persist the log, the per-actor sequences and the outcome window.
- **Grants control writes, not reads.** Every replica, the agent's included, pulls the whole workspace: all records, all fields and every open proposal. Read scoping (per-actor projections of the snapshot and the log) is not implemented; a production server must filter what an agent can read.
- **The library does no I/O.** `ReplicaState` is `Codable`; storing it is the host's job, through the `persist` handler (called before every push) and its own saves.
- **Restoring an older save than the last one stored can lose edits** (a lost write, a restore from backup). The replica may then hand out keys the server already holds. Nothing is applied twice, but the edit under such a key is lost: a different body is refused as `.idempotencyConflict` and listed in `rejections`; an identical body is taken to be the old edit and reported as *committed* with the old outcome, without being applied again, even if that field has changed since. After its first pull a restored replica moves its next key past every key the log shows, which narrows this to keys whose only outcome was a refusal (refusals never enter the log).
- **The replay-window fingerprint is not cryptographic,** and it hashes JSONEncoder's output, which is stable on the toolchains CI runs but not promised across Foundation versions. A collision could only replay an actor's own earlier outcome to that same actor; a server that persists fingerprints should store an encoding version with them.
- **The agent in the demo is scripted.** `ScriptedNotesAgent` returns fixed `tool_use` inputs, including two deliberate mistakes. No model is called; the point is the seam, which takes real tool-use input unchanged.
- **Accepted proposals to `lastWriterWins` fields overwrite newer values.** That's what the policy says; make a field `rejectStale` if an approved agent edit must not land on top of a newer human one.
- **Tombstones are never purged**, and compaction keeps the whole current state, so the log is bounded but the state grows with the data.
- **No transport, auth or encryption.** `SyncTransport` is the seam; `LocalTransport` is in-process. Identity comes from the transport's credential, so a real one must authenticate the caller.
- **Not built for collaborative rich text.** Field-level rules are the wrong grain for character-level merging; use a text CRDT for that and this for the records around it.

## Verification

Run locally on this package: a clean `swift build --build-tests -Xswiftc -warnings-as-errors` with zero warnings, `swift test` with 80 tests passing, `xcodebuild build` of both products for a generic iOS Simulator with warnings as errors (Xcode 27.2), and `scripts/mutation-check.sh` catching all 42 mutants. The CI workflow runs the same on GitHub's Linux (swift 6.0) and macOS 15 (Xcode 16.4) runners; see the [Actions tab](https://github.com/rajatslakhina/agent-peer-sync-kit/actions) for the latest results.

## Install

```swift
.package(url: "https://github.com/rajatslakhina/agent-peer-sync-kit.git", from: "3.0.0")
```

MIT licensed.
