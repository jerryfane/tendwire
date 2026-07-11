# Goal 11: Make Request Identity the Command Idempotency Contract

## Objective

Prevent legitimate repeated instructions from being suppressed while preserving
exactly-once behavior for transport retries and uncertain command results.

## Confirmed Defect

`src/tendwire/command_submission.py::_duplicate_instruction_envelope` suppresses
same-worker, same-text instructions of at least 40 characters for six hours,
even when the caller supplies a new request ID. Repeating a legitimate long
instruction intentionally can therefore return a successful-looking
`duplicate_suppressed` result without reaching the worker.

The historical query also examines stored request targets by `worker_id`, so
behavior differs depending on whether a caller originally selected by worker,
name, or space. Content similarity is not a reliable transport identity.

## Required Semantics

1. `request_id` is the authoritative idempotency key for mutating commands.
2. Same request ID plus the same canonical request returns the stored/reserved
   result and never sends twice.
3. Same request ID plus a different canonical request is rejected as
   `duplicate_request` (or the existing equivalent) before backend mutation.
4. Different request IDs are distinct user intentions, even when target and text
   are identical. Both must be submitted unless the caller explicitly requests
   a separately documented safety policy.
5. Remove automatic six-hour content suppression from the normal path. If a
   content replay guard is retained as opt-in defense, it must use a short,
   configured window, canonical resolved target, explicit override, and a
   non-success status that tells the caller nothing was sent.
6. Resolve selectors authoritatively before reservation/send, then persist the
   canonical public worker identity and private routing binding separately.
   Name/space and worker-ID selectors must have equivalent idempotency behavior.
7. A transport timeout after send begins remains `request_state_uncertain`.
   Retrying with the same request ID must resolve/replay the stored state, not
   perform a blind second send.
8. Ingress connectors must reuse one opaque request ID for retries of the same
   inbound message. Herdres should derive it from private Telegram identity with
   a keyed/opaque transform; raw chat/topic/message IDs must not enter Tendwire
   public JSON.
9. Command events and receipts must clearly distinguish reserved, send-started,
   submitted/accepted, uncertain, rejected, and duplicate-request states. Never
   report `accepted` for an instruction intentionally not sent.
10. Keep request records bounded with retention that outlives the maximum retry
    horizon. Do not prune idempotency evidence while a connector can still retry.

## Implementation Quality Constraints

- Keep one canonical request fingerprint and one request-ID reservation state
  machine. CLI, daemon, and connector paths must call it rather than duplicate
  dedupe rules.
- Remove normal-path content suppression and its dead query/config constants if
  no explicit opt-in policy remains. Do not leave unreachable legacy heuristics.
- Represent reservation, send-started, accepted, uncertain, and rejected as
  explicit transitions with one terminal-result writer.
- Never convert a non-send into `ok=True` for compatibility. Compatibility
  mapping belongs at the client edge and must retain truthful status.
- Put opaque ingress request-ID derivation in one private Herdres helper; do not
  leak Telegram structure into Tendwire or duplicate HMAC recipes.
- Use deterministic barriers for reservation races and transport uncertainty;
  avoid timing sleeps and mock-only state assertions.

## Required Tests

- Submit the same 100-character instruction twice with two request IDs; backend
  receives exactly two sends and both results are accepted.
- Retry the same request ID and payload many times before, during, and after
  completion; backend receives exactly one send.
- Reuse the request ID with changed text, target, action, or relevant options;
  every variant is rejected without backend mutation.
- Repeat through worker-ID, name, and space selectors; semantics are identical
  after authoritative resolution.
- Simulate timeout before send, during send, and after send; retry behavior
  follows the documented certainty state without blind duplication.
- Two deliveries of one Telegram update derive the same opaque request ID;
  different Telegram updates with identical text derive different IDs.
- Public payloads contain no raw Telegram identifiers or private target values.
- Receipt retention covers the configured connector retry horizon.

Retire or rewrite tests that assert different request IDs should be
content-suppressed. Preserve all same-request idempotency and reservation-race
tests.

## Acceptance Evidence

- Focused command, receipt, CLI, and concurrency tests pass.
- Full Tendwire suite passes.
- An exactly-once race test records one backend send and one accepted receipt for
  many concurrent same-ID callers.
- A distinct-ID repeat test records two intentional sends.

## Non-Goals

- Do not add semantic/LLM similarity detection.
- Do not expose Telegram identity to Tendwire clients.
- Do not deploy, merge, or restart services.
