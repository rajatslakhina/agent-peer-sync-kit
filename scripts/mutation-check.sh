#!/usr/bin/env bash
# Each mutant re-introduces a specific bug that the README says the design prevents.
# A mutant "survives" if the test suite still passes with it applied, which means the
# claim is untested. The script fails if any mutant survives, if a mutant doesn't
# compile, if a pattern isn't found exactly once, or if the unmutated suite fails.
# Runs on macOS and Linux (the library has no Apple-only code).
set -u
cd "$(dirname "$0")/.."
S=Sources/AgentPeerSync
# file | exact text to replace (must occur exactly once; \n is a newline) | replacement | the claim it breaks
MUTANTS=(
  '$S/CommitLog.swift|        if seq <= last {|        if false, seq <= last {|a retry replays the stored outcome instead of being re-applied'
  '$S/CommitLog.swift|            return stored.fingerprint == Fingerprint(mutation) ? stored.outcome : .rejected(.idempotencyConflict)|            return stored.outcome|a reused idempotency key with a different body is refused'
  '$S/CommitLog.swift|        if seq > retention { stored.removeValue(forKey: seq - retention) }|        _ = retention|stored outcomes are bounded by the replay window'
  '$S/CommitLog.swift|        lastSeq[caller] = seq|        if case .rejected = outcome {} else { lastSeq[caller] = seq }|a rejection consumes its sequence number'
  '$S/CommitLog.swift|        guard mutation.id.actor == caller else { return .rejected(.actorMismatch) }|        _ = caller|a caller cannot submit another actor'"'"'s mutation'
  '$S/CommitLog.swift|                resolution = recheck(proposal)|                resolution = .applied|acceptance is re-checked against the current state and grant'
  '$S/CommitLog.swift|            guard proposal.author != author else { return .rejected(.selfReview) }|            _ = proposal.author|an actor cannot accept its own proposal'
  '$S/CommitLog.swift|        if entries.count > configuration.maxRetainedEntries { compact(keepingLast: configuration.maxRetainedEntries / 2) }|        _ = configuration.maxRetainedEntries|the log compacts itself to bound memory'
  '$S/Policy.swift|               let write = existing?.writes[name], write.version > baseVersion, write.actor != author {|               let write = existing?.writes[name], false, write.version > baseVersion, write.actor != author {|a stale write to a rejectStale field is refused'
  '$S/Policy.swift|               let write = existing?.writes[name], write.version > baseVersion, write.actor != author {|               let write = existing?.writes[name], write.version > baseVersion {|an author'"'"'s own earlier write is not a conflict'
  '$S/Policy.swift|        guard open < policy.maxOpenProposals else {|        guard true else {|open proposals per author are bounded'
  '$S/Policy.swift|            case .reviewed?: needsReview = true|            case .reviewed?: break|a reviewed field turns the whole write into a proposal'
  '$S/State.swift|        return rhs > 0 ? .max : .min|        return sum|counters saturate instead of wrapping'
  '$S/State.swift|        appliedThrough[author.actor] = max(appliedThrough[author.actor] ?? 0, author.clientSeq)|        _ = author|the reducer records which edits the log already holds'
  '$S/Replica.swift|            if seq > confirmed.version { committedAhead.append(CommittedEdit(mutation: mutation, seq: seq)) }|            _ = seq|an acknowledged edit stays visible until it is pulled'
  '$S/Replica.swift|        committedAhead.removeAll { $0.seq <= confirmed.version }|        committedAhead.removeAll { _ in false }|pulled commits leave the overlay so newer remote values show'
  '$S/Replica.swift|            return mutation.id.clientSeq > admitted|            return true|an edit whose response was lost is not counted twice'
  '$S/Replica.swift|        let overlay = committedAhead.sorted { $0.seq < $1.seq }.map(\.mutation) + unanswered|        let overlay = committedAhead.sorted { $0.seq < $1.seq }.map(\.mutation)|local edits are visible before they sync'
  '$S/Replica.swift|        guard let running = inFlight else { return try await startOrJoin() }|        guard let running = Optional<Task<SyncReport, any Error>>.none else { return try await startOrJoin() }|concurrent sync calls join the running sync'
  '$S/Replica.swift|        } catch {\n            pushFailure = error\n        }|        } catch {\n            throw error\n        }|a failed push does not stop the pull'
  '$S/AgentToolSurface.swift|            guard let kind = schema.fields[field], let grant = policy.grant(for: field) else { return nil }|            guard let kind = schema.fields[field], let grant = Optional(policy.grant(for: field) ?? .direct) else { return nil }|the tool offers only the fields the actor may write'
  '$S/AgentToolSurface.swift|let delta = Int64(exactly: number),|let delta = Optional(Int64(number)),|a counter delta that does not fit Int64 is refused, not trapped'
  '$S/AgentToolSurface.swift|                try Self.requireOnly(["field", "value"], in: body)\n                guard case let .string(text)?|                guard case let .string(text)?|unexpected keys in a text edit are refused'
  '$S/State.swift|        guard version < UInt64.max, entry.seq == version + 1 else { return false }|        guard version < UInt64.max, entry.seq > version else { return false }|a replica never applies an entry past a gap'
  '$S/Replica.swift|        guard outbox.count + committedAhead.count < Self.maxPendingEdits else {|        guard true else {|pending edits are bounded'
  '$S/Replica.swift|used.allSatisfy({ $0.actor == actor && $0.clientSeq < state.nextSeq })|used.allSatisfy({ $0.actor == actor })|a restore that would reuse idempotency keys is refused'
  '$S/CommitLog.swift|            guard index < configuration.maxPushBatch else {|            guard true else {|a push is cut at maxPushBatch'
  '$S/CommitLog.swift|compact(keepingLast: configuration.maxRetainedEntries / 2)|compact(keepingLast: 0)|auto-compaction keeps a tail, so slightly-behind replicas get entries'
  'Sources/AgentPeerSyncTesting/Workspace.swift|matchesServer: view.confirmed == truth|matchesServer: true|the convergence check can report divergence'
  '$S/Replica.swift|        case .rejected(.batchTooLarge):\n            break // not looked at by the server: stays queued for the next round\n||edits cut from an oversized batch stay queued'
  '$S/Replica.swift|                        report.pullStalled = true\n||a gap in pulled history is reported'
  '$S/Replica.swift|        let admitted = confirmed.appliedThrough[actor] ?? 0\n        if admitted < UInt64.max, nextSeq <= admitted { nextSeq = admitted + 1 }|        _ = confirmed|a pull moves the next key past edits the log already holds'
  '$S/AgentToolSurface.swift|                      delta >= -Self.maxCounterDelta, delta <= Self.maxCounterDelta else {|                      true else {|counter deltas beyond 2^53 - 1 are refused'
  '$S/AgentToolSurface.swift|        guard !editShapes.isEmpty else { return nil }|        _ = editShapes.isEmpty|a policy that offers nothing has no tool'
  '$S/Replica.swift|        guard outbox.contains(where: { $0.id.clientSeq <= newestAtCall }) else { return report }|        if true { return report }|a joiner whose edit missed the running push gets another round'
  '$S/Replica.swift|                if confirmed.version == cursor {\n                    report.pullStalled = true|                if confirmed.version == cursor {\n                    report.pullStalled = false|a page that makes no progress is reported'
  '$S/Replica.swift|        guard fresh.contains(where: { outbox[$0].id.clientSeq <= admitted }) else { return }|        guard false else { return }|edits queued after a restore are re-keyed past the log'"'"'s keys'
  '$S/Replica.swift|        if freshFrom != nil {\n            try await pullToHead|        if false, freshFrom != nil {\n            try await pullToHead|a restored replica pulls before its first push'
  '$S/CommitLog.swift|        let fingerprint: Fingerprint\n        let outcome: MutationOutcome\n|        let fingerprint: Fingerprint\n        let outcome: MutationOutcome\n        let body: Mutation? = nil\n|the replay window never holds the body'
  '$S/Replica.swift|            if report.pullStalled { return report }\n            rekeyEditsQueuedSinceRestore()|            rekeyEditsQueuedSinceRestore()|a restored replica pushes nothing until its first pull reaches the head'
  '$S/Replica.swift|state.actor == actor, state.nextSeq >= 1,|state.actor == actor,|a restored state with nextSeq 0 is refused'
  '$S/Replica.swift|        report.pullStalled = true // page budget exhausted before reaching the head\n||running out of page budget is reported'
  '$S/AgentToolSurface.swift|        if policy.mayDelete { lines.append(|        if false, policy.mayDelete { lines.append(|the tool description mentions delete when it is offered'
  '$S/Replica.swift|            if let boundary = freshFrom, mutation.id.clientSeq >= boundary { return true }\n||a restored replica shows its fresh edits before its first full pull'
  '$S/Replica.swift|        freshFrom = state.freshFrom.map { min($0, next) } ?? next|        freshFrom = next|the fresh-key boundary survives a second relaunch'
  '$S/CommitLog.swift|        public var outcomeRetention: Int { didSet { outcomeRetention = min(max(outcomeRetention, 1), 100_000) } }|        public var outcomeRetention: Int|configuration set after init is clamped'
)
if command -v timeout >/dev/null; then LIMIT="timeout 300"; else LIMIT="perl -e alarm(300);exec(@ARGV)"; fi
if ! swift build --build-tests -q >/dev/null 2>&1 || ! $LIMIT swift test --skip-build >/dev/null 2>&1; then
  echo "BASELINE FAILED: the unmutated suite must pass before mutants mean anything"; exit 2
fi
echo "baseline: unmutated suite passes"
survived=0; caught=0
BACKUP=$(mktemp -d); CURRENT=""
trap '[ -n "$CURRENT" ] && cp "$BACKUP/orig" "$CURRENT"; rm -rf "$BACKUP"' EXIT
skip=${SKIP:-0}; index=0  # SKIP=n resumes after n mutants on an unchanged tree
for entry in "${MUTANTS[@]}"; do
  index=$((index + 1)); [ "$index" -le "$skip" ] && continue
  entry=${entry//\$S/$S}
  IFS='|' read -r file from to claim <<< "$entry"
  cp "$file" "$BACKUP/orig"; CURRENT="$file"
  count=$(FROM="$from" perl -0777 -ne '($f=$ENV{FROM}) =~ s/\\n/\n/g; $c = () = /\Q$f\E/g; print $c' "$file")
  if [ "$count" != "1" ]; then echo "SETUP ERROR: pattern occurs $count times in $file: $claim"; exit 2; fi
  FROM="$from" TO="$to" perl -0777 -pi -e 'BEGIN { ($f = $ENV{FROM}) =~ s/\\n/\n/g; ($t = $ENV{TO}) =~ s/\\n/\n/g } s/\Q$f\E/$t/' "$file"
  if ! swift build --build-tests -q >/dev/null 2>&1; then
    echo "SETUP ERROR: mutant does not compile: $claim"; exit 2
  fi
  $LIMIT swift test --skip-build >/dev/null 2>&1; status=$?
  if [ "$status" -eq 0 ]; then
    echo "SURVIVED: $claim"; survived=$((survived + 1))
  elif [ "$status" -eq 124 ] || [ "$status" -eq 142 ]; then
    # A hang is not a catch: the suite must fail on an assertion, not on a timeout.
    echo "TIMEOUT:  $claim"; survived=$((survived + 1))
  else
    echo "caught:   $claim"; caught=$((caught + 1))
  fi
  cp "$BACKUP/orig" "$file"; CURRENT=""
done
echo "mutants caught: $caught, survived or timed out: $survived"
[ "$survived" -eq 0 ]
