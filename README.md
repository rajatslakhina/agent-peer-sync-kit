# AgentPeerSync

**Your AI agent is just another offline device.** A local-first sync engine for an iOS app where a Claude-style agent writes through the same mutation log as the user's phones: its own replica, its own cursor, its own idempotency keys, and a server-side grant that decides which of its edits apply at once and which wait for a person.

Demo app: (added after the companion repo is pushed — see below)

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
| `CommitLog` (actor) | The authority. `push(_:as:)` admits a batch for an authenticated caller; `pull(after:limit:)` pages the log or returns a snapshot. Compacts itself after `maxRetainedEntries`. |
| `Replica` (actor) | One per device or agent. `write` / `review` queue a mutation with a fresh idempotency key; `sync()` pushes, then pulls and rebases. `view()` returns confirmed state, the optimistic `visible` state, the outbox and refusals. `persistentState()` / `init(restoring:)` survive a relaunch. |
| `Admission` | The server's rules as a pure function: field kinds, text limits, grants, stale-write detection, delete/create rights, proposal backpressure. |
| `ActorPolicy` | Per actor, held by the server: `directFields`, `reviewedFields`, `mayCreate`, `mayDelete`, `mayReview`, `maxOpenProposals`. |
| `Snapshot` | The shared state at a version: records, open proposals, and per-actor `appliedThrough`. Built by one reducer on the server and on every replica. |
| `AgentToolSurface` | Turns an agent's policy into a Messages API tool definition (`name`, `description`, `input_schema`) and parses `tool_use` input strictly into ops. |
| `SyncTransport` | The seam to the network. The transport carries the credential, so the replica can't choose who it is. |
| `AgentPeerSyncTesting` | `LocalTransport` with fault injection (`dropRequest`, `dropResponse`, `duplicateRequest`), the sample workspace, and the scripted day the demo plays. For tests and demos, not production. |

### Using it

```swift
let surface = AgentToolSurface(schema: schema, policy: agentPolicy)
// Send surface.definition().json in `tools`. For each tool_use block that comes back:
switch await surface.submit(toolUse.input, to: agentReplica) {
case .queued(let id):        break                  // in the agent's outbox, like a phone edit
case .refused(let error):    reportToModel(error)   // never reached the outbox
case .replicaFailed(let e):  handle(e)
}
try await agentReplica.sync()                       // the server decides: commit, propose or reject
```

## Design decisions and trade-offs

**Conflict resolution happens once, on the server, at admission.** Replicas never re-decide anything; they replay admitted entries with the same reducer (`Snapshot.apply`). Convergence is then a property of the log, not of each client's merge code. *Rejected:* client-side CRDT merge for everything. It's the right tool for peer-to-peer or rich text, but a CRDT can't express "this agent may only propose title changes" or "a person must approve", and authorization is the point here.

**Per-field conflict policy instead of one rule.** `rejectStale` for fields where a silent overwrite destroys someone's work (titles), `lastWriterWins` for cheap fields (notes, flags), and counters that only change by `add`, so concurrent offline votes all count. "Last" means last admitted by the server, not a wall clock: device clocks are not trusted. An actor's own earlier write never makes its next write stale.

**Field-level trust tiers for the agent.** The sample agent writes its own `summary` field directly, may only *propose* changes to `title` and `notes`, and can't create, delete, vote, tick items off or review. A mutation that touches any reviewed field becomes a proposal as a whole (mutations are atomic). Proposals are bounded per author (`maxOpenProposals`), which is backpressure on an agent that floods the queue.

**Acceptance is re-checked against the state now.** When a person accepts, the server re-runs admission against the *current* state and the author's *current* policy. If a person edited a guarded field while the proposal waited, the record was deleted, or the agent's grant was revoked, the proposal closes as `.superseded(reason)` instead of overwriting newer work. *Rejected:* applying the proposal as it was approved in the UI; the UI's copy is exactly the stale view this prevents.

**Review decisions are mutations.** Accept/reject travels through the same outbox, keys and log, so a reviewer can approve offline and the decision is ordered and deduplicated like any edit. No actor can settle its own proposal.

**Idempotency is a per-writer sequence, and rejections consume it.** The server keeps the last `outcomeRetention` outcomes per actor (default 1,024) and replays them for retries; older retries get `.retryExpired`, never a second application. Because a refusal still consumes its number, a client never has to resubmit a refused edit to make progress. *Rejected:* random UUID keys with an unbounded seen-set — a contiguous sequence also gives per-writer ordering and a bounded window for free.

**The optimistic overlay is honest about what it knows.** `visible` is confirmed state plus (a) edits the server committed at a version the replica hasn't pulled yet, so an acknowledged edit doesn't flicker out, and (b) unanswered edits — except ones the log already contains (`appliedThrough`), so an edit whose response was lost isn't counted twice after a pull. A failed push doesn't stop the pull, so pending edits still rebase onto fresh state during a write outage.

**The agent's tool comes from its grant.** `AgentToolSurface` only offers the fields the policy allows and says in the description which apply immediately. `decode` refuses unknown keys, unoffered fields, wrong types and counter deltas that don't fit `Int64` (via `Int64(exactly:)`, never a trapping conversion). This is defence in depth: the server checks everything again.

**Bounded everywhere.** Identifiers are 1–64 ASCII characters; text limits are counted in Unicode scalars (the unit JSON Schema's `maxLength` uses) and clamped; ops per mutation, open proposals, the replay window, the retained log, pull pages, the replica's pending edits (10,000) and its refusal history (50) all have limits. Counters saturate at `Int64.min`/`.max` instead of trapping.

## What's tested

65 XCTest tests, run on macOS and Linux in CI, plus a **mutation check**: `scripts/mutation-check.sh` re-introduces 26 specific bugs, one at a time (skip the idempotent replay, apply an accepted proposal without re-checking it, let a stale write through, offer the agent every field, drop the outbox overlay, stop coalescing concurrent syncs, apply an entry past a gap, …) and fails CI if any of them still passes the suite.

- **Admission** (18): every rule above, including own-write-isn't-stale, scalar-counted text limits and proposal backpressure.
- **Commit log** (15): replay, key reuse, gaps, sequence consumption, spoofed authors, revocation, replay-window expiry, review permissions, re-check on accept, paging, compaction.
- **Replica** (12) and **persistence** (6): overlay, no-flicker, rebase-on-pull during a write outage, lost and duplicated responses, coalesced syncs, relaunch from saved state, stale-save refusal, bounded outbox, gap refusal.
- **Agent tool** (9): offered fields match the grant, strict decoding, `Int64` edges, and every offered edit is admitted by the server.
- **Convergence** (3): 36 seeded random schedules of offline edits, reviews, connectivity changes and transport faults across two phones and an agent (some with a tiny replay window and aggressive compaction). Each must end with every replica equal to the server, a fresh replica paging one entry at a time equal to the server, and (in the runs without compaction, where the whole log is still there to inspect) no mutation in the log twice and each actor's mutations in its own order. A companion test feeds a deliberately broken server and asserts the equality check fails.
- **Scenario** (2): the exact script the demo app plays, checked beat by beat.

## Honest limits

- **The server here is a reference implementation in memory.** `CommitLog` shows the admission and replay rules; a production server would run the same rules inside a database transaction and persist the log, the per-actor sequences and the outcome window.
- **The library does no I/O.** `ReplicaState` is `Codable`; persisting it (and doing so *before* an edit can reach the network) is the host's job. A replica restarted from a stale save gets `.idempotencyConflict` for reused keys rather than silent corruption, but those edits are lost.
- **The agent in the demo is scripted.** `ScriptedNotesAgent` returns fixed `tool_use` inputs, including two deliberate mistakes. No model is called; the point is the seam, which takes real tool-use input unchanged.
- **Accepted proposals to `lastWriterWins` fields overwrite newer values.** That's what the policy says; make a field `rejectStale` if an approved agent edit must not land on top of a newer human one.
- **Tombstones are never purged**, and compaction keeps the whole current state, so the log is bounded but the state grows with the data.
- **No transport, auth or encryption.** `SyncTransport` is the seam; `LocalTransport` is in-process. Identity comes from the transport's credential, so a real one must authenticate the caller.
- **Not built for collaborative rich text.** Field-level rules are the wrong grain for character-level merging; use a text CRDT for that and this for the records around it.

## Verification

Run locally on this package: a clean `swift build --build-tests -Xswiftc -warnings-as-errors` with zero warnings, `swift test` with 65 tests passing, `xcodebuild build` of both products for a generic iOS Simulator with warnings as errors, and `scripts/mutation-check.sh` catching all 26 mutants. The CI workflow runs the same on GitHub's Linux (swift 6.0) and macOS 15 runners; see the [Actions tab](https://github.com/rajatslakhina/agent-peer-sync-kit/actions) for the latest results.

## Install

```swift
.package(url: "https://github.com/rajatslakhina/agent-peer-sync-kit.git", from: "1.0.0")
```

MIT licensed.
