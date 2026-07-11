# Goal 01: Correct Stable Worker Continuity

## Objective

Replace the recently added `meta.stable_key` implementation with a genuinely
restart-stable, opaque worker continuity handle, and harden the Tendwire/Herdres
contract so an observed payload cannot inject that handle.

## Confirmed Defect

`src/tendwire/backends/herdr_cli.py::_stable_pane_identity` prefers
`terminal_id` and comments that the value survives a Herdr restart. That premise
is false for the reviewed Herdr implementation: restore allocates new terminal
registry IDs. Split panes also receive distinct terminal IDs, contrary to the
current comments.

Herdr does persist its public pane numbering in snapshot state. That persisted
logical pane identity, not a newly allocated terminal registry ID, is the
available restart-continuity source.

There are two additional contract problems:

- `_worker_with_stable_key` serializes output from
  `worker_binding_private_fingerprint`, although that helper is documented as
  private and non-serializable.
- `_meta_from_item` accepts arbitrary inbound `meta.stable_key`. When no local
  identity can be derived, `_worker_with_stable_key` returns the worker
  unchanged, allowing an untrusted source value to survive into public JSON.

The current assumptions are encoded in `tests/test_worker_stable_key.py`, so a
green existing suite does not prove correctness.

## Required Design

1. Confirm the exact Herdr field or canonical tuple that represents persisted
   public pane identity using current Herdr schema/restore fixtures. Do not infer
   durability from a field name.
2. Derive continuity from that persisted pane identity plus the minimum scope
   needed to prevent collisions, such as host and workspace identity.
3. The published value must be opaque and domain-separated from private binding
   fingerprints. Prefer an installation-local keyed HMAC with a versioned public
   format such as `wsk1_<digest>`. An unsalted hash of low-entropy pane material
   is not sufficient.
4. Raw identity material remains private. No pane ID, terminal ID, session ID,
   backend target, or recognizable substring may appear in the public handle.
5. Reserve Tendwire-owned metadata keys. Drop inbound `stable_key`,
   `stable_key_version`, and any future names in the same reserved namespace
   before constructing a `Worker`.
6. Locally derived metadata is added only after source sanitization. If no
   authoritative continuity source exists, omit the key; never preserve a
   source-provided fallback.
7. Document move semantics. Decide whether the key follows a logical pane across
   workspace moves or intentionally changes, based on Herdr's persisted identity.
   Add a regression for the selected behavior.
8. Update Herdres' source-mode reconciliation to validate the versioned key,
   reject malformed or source-spoofed values, and avoid fusing two simultaneous
   workers. Herdres must not query Herdr directly to compensate.
9. Preserve existing Telegram topics during migration. Reconcile an already
   bound live worker to the new key once, update the private topic binding, and
   do not create a duplicate topic or repost old turns.

If a stable installation key does not already exist, add one with secure
creation and permissions under Goal 04's contract. Do not derive it from a
public host name or check it into configuration examples. The key must survive
ordinary upgrades/restarts and have documented backup/rotation behavior; losing
it must never cause silent cross-worker rebinding.

## Required Tests

Add deterministic fixtures for all of the following:

- same persisted pane identity before/after restore, with changed worker,
  agent, terminal, and session IDs, yields the same public handle;
- two split panes remain distinct;
- sibling close/reordering does not change the surviving pane's handle;
- the documented workspace-move behavior is stable;
- two hosts/installations cannot be linked by the same public handle;
- an ordinary Tendwire upgrade/restart retains the local key and public handle;
- deliberate key rotation cannot silently bind an old topic to the wrong pane;
- a payload containing attacker-controlled `meta.stable_key` cannot publish it,
  including when no local identity is available;
- public serialization contains no raw source identity or private fingerprint;
- malformed/version-unknown keys are ignored safely by Herdres;
- migration reuses one existing topic, does not duplicate it, and does not
  replay old turns;
- live same-key collision handling fails closed rather than misrouting input.

Use a restore-shaped Herdr fixture that reflects the real `EventEnvelope` and
snapshot fields. Remove or rewrite comments/tests asserting terminal-ID
durability.

## Acceptance Evidence

- Focused Tendwire and Herdres tests pass.
- Full Tendwire and full hermetic Herdres suites pass.
- A public JSON sentinel scan reports zero forbidden identity values.
- A fixture restart proves continuity while terminal IDs change.
- Herdres source diagnostics still report `direct_herdr_calls=0`.

## Non-Goals

- Do not make raw pane identity public.
- Do not add pseudo pane IDs.
- Do not change Telegram topic policy beyond continuity migration.
- Do not restart Herdr, deploy, or merge this branch.
