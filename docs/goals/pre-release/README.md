# Tendwire Pre-Release Remediation Goal Pack

This directory turns the 2026-07-10 release review into implementation
contracts. Each numbered document is an independently reviewable goal. The
documents describe required behavior and proof; they are not permission to
merge, deploy, restart services, or broaden product scope.

## Reviewed Baseline

- Tendwire: `main` / `origin/main` at
  `21cddcbbb1a05aaea72287359c241666020dcb6b`.
- Herdres: `main` / `origin/main` at
  `21040f6035b1ac37d79f22aa08dec6c67671d688`.
- Tendwire full suite: `810 passed`.
- Focused release and PR regressions: `20 passed`.
- Herdres stable-worker consumer regressions: `30 passed`.
- Offline Herdr fixture smoke: `10/10` checks passed.
- Python compile checks passed under the available Python 3.11 and 3.13
  interpreters.
- The source archives and repository secret-pattern scan were clean at review
  time.

These results are a baseline, not a substitute for the goal-specific tests.
An implementer must record the exact base commit because `main` can move.

## Global Contract

Every goal in this pack must preserve these invariants:

1. `HERDRES_TENDWIRE_MODE=source` remains the normal production path.
2. Herdres source mode must not call direct Herdr pane APIs. This includes
   `pane_list`, `pane_by_id`, `pane_turn`, `prefetch_pane_turns`,
   `send_to_pane`, `herdr pane send-keys`, and `herdr pane read`.
3. Tendwire public JSON must not expose pane IDs, terminal IDs, backend targets,
   raw target values, private fingerprints, Telegram chat/topic/message IDs,
   socket paths, argv, environment values, stdout/stderr, tokens, or secrets.
4. Do not create synthetic pane IDs such as `tendwire:*`.
5. Telegram delivery and presentation remain Herdres-owned. Tendwire owns Herdr
   observation, private bindings, turns and pending state, attention, command
   routing, receipts, backend health, event/projection state, and the neutral
   connector outbox.
6. `legacy`, `off`, and `enrich` are rollback/debug modes only. Do not add new
   behavior to them unless a goal explicitly requires a compatibility fix.
7. Do not start Hermes, MCP, iOS, AR, or unrelated UI work.
8. Never restart `herdr-server.service` from these goals. Herdr checks are
   status-only: `systemctl --user is-active herdr-server.service` and, only when
   needed, `systemctl --user status herdr-server.service`.
9. Do not merge or deploy an implementation before independent review. Do not
   restart Tendwire or Herdres services as part of implementation review.
10. Never place real credentials or provider-shaped literal test secrets in the
    repository. Construct detector fixtures from harmless fragments at runtime.

## Implementer Protocol

Use one goal per branch or temporary worktree. Keep the diff narrow and retain
the repository's existing zero-runtime-dependency posture unless a goal proves
a dependency is necessary.

Before coding, the implementer must:

- fetch current origin state and record the exact base commit;
- reproduce the defect with a focused failing test or deterministic probe;
- inspect overlapping open work before editing shared files;
- state any required public-schema or SQLite migration explicitly.

Before handing the work back for review, the implementer must report:

- branch and commit hash;
- exact files changed;
- behavioral decisions and deviations from this contract;
- focused test commands and results;
- full Tendwire test result;
- any Herdres tests when the public contract or connector behavior changed;
- migration, rollback, performance, and privacy evidence;
- confirmation that nothing was merged, deployed, or restarted.

Do not hide a partial result behind a green focused test. If an acceptance item
is not proven, mark it unresolved.

## Engineering Quality Gate

Passing tests is necessary but not sufficient. Implementations will be rejected
when they solve the immediate fixture by making the production design harder to
understand, maintain, or safely remove.

1. Make the smallest coherent change that fixes the invariant. Prefer deleting
   obsolete logic over leaving old and new paths active behind flags.
2. Follow existing ownership boundaries and repository patterns. Do not add a
   framework, manager, registry, generic utility layer, or configuration option
   unless the goal demonstrates more than one real use and the abstraction
   removes more complexity than it adds.
3. Keep one authoritative implementation for each rule. Sanitization,
   identity, retries, retention, migrations, and state transitions must not be
   reimplemented slightly differently at multiple call sites.
4. Use explicit domain outcomes instead of overloaded `None`, boolean piles,
   broad `except Exception`, or silent fallback. A safety failure must remain
   distinguishable from absence, compatibility mode, and transient failure.
5. Keep functions cohesive and data flow visible. Do not add pass-through
   wrappers, speculative interfaces, dead extension points, or deeply nested
   control flow merely to make the diff look structured.
6. Comments and docstrings explain non-obvious invariants, threat boundaries,
   or protocol decisions. Do not narrate obvious assignments or add long
   generated-looking prose inside source files.
7. Every new module, table, public field, environment variable, dependency, and
   long-lived cache requires a concrete justification in the handoff. Prefer
   standard-library and existing helpers.
8. Tests must exercise public behavior and the original failure. Do not mock the
   function being proved, copy production logic into expected values, weaken an
   assertion, add timing sleeps where a barrier works, or preserve a bug as the
   new expected result.
9. Keep compatibility code thin, explicit, and removable. It must not become a
   second normal implementation path.
10. Include `git diff --stat` and explain significant added complexity, removed
    obsolete code, and any function/module that grew materially. Review may
    require simplification even when behavior is correct.

There is no arbitrary line-count target: concise code that hides state is not an
improvement. The standard is minimal concepts, explicit invariants, and no
duplicated mechanism.

## Goal Order

| Goal | Subject | Dependency | Primary risk |
| --- | --- | --- | --- |
| [01](01-stable-worker-continuity.md) | Stable worker continuity | Cross-repo; coordinate with 04 if adding a key | Topic duplication/misrouting |
| [02](02-real-herdr-event-deduplication.md) | Real Herdr event dedupe | None | Lost state transitions |
| [03](03-public-turn-content-safety.md) | Public content safety | None | Private data disclosure |
| [04](04-local-state-permissions.md) | Local state permissions | None | Local credential/data exposure |
| [04B](04b-attention-lifecycle-stability.md) | Attention lifecycle stability | Complete 04 first | Repeated/flapping notifications |
| [05](05-lossless-long-responses.md) | Lossless long responses | Coordinate with 07 and 10 | User-visible truncation |
| [06](06-bounded-store-and-migrations.md) | Store growth and migrations | None | Disk/latency growth |
| [07](07-background-turn-ingestion.md) | Background turn ingestion | Prefer 06 first | API stalls/races |
| [08](08-codex-session-reader-hardening.md) | Codex reader safety/performance | None; integrate with 07 | Cross-session reads/high I/O |
| [09](09-independent-pending-state.md) | Independent pending state | 07 | Missing/stale approvals |
| [10](10-delivery-aware-turn-retention.md) | Delivery-aware retention | Coordinate after 07 | Permanent message loss |
| [11](11-command-idempotency-semantics.md) | Command idempotency | None | Legitimate prompt suppression |
| [12](12-release-engineering.md) | CI, packaging, RC proof | All accepted goals | Unrepeatable release |

Goals `01` through `05`, `08`, and `11` can be implemented independently when
their touched files do not overlap. Goal `04B` must follow acceptance of Goal
`04`; it also changes store lifecycle code and must not overlap Goals `06`,
`07`, `09`, or `10` unless deliberately stacked. Those four goals all change
store/daemon ownership and must be serialized. Goal `12` is the final release
gate.

## Review Standard

Review will evaluate behavior, not merely whether requested symbols were added.
For each branch, the reviewer should:

1. Reproduce the original defect against the base commit.
2. Inspect the complete diff and migrations.
3. Run the focused regressions and the full hermetic suite.
4. Probe public JSON with forbidden sentinel values.
5. Check concurrency, restart, retry, and rollback behavior relevant to the
   goal.
6. Reject unrelated refactors, weakened tests, test-only bypasses, or behavior
   that moves Herdr/Telegram ownership across the established boundary.

Completion of all documents means the code is eligible for an RC proof. It does
not itself authorize a release.
