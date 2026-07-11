# Goal 08: Harden and Incrementalize the Codex Session Reader

## Objective

Prevent wildcard/cross-session file selection and stop rereading multi-megabyte
Codex JSONL histories on every poll.

## Confirmed Defect

`src/tendwire/backends/herdr_turns.py::_safe_codex_session_id` rejects path
separators and dot segments but accepts glob metacharacters. `_find_codex_session_file`
then interpolates the value into `rglob`. A probe using `*` selected an arbitrary
session file.

The reviewed machine had more than 23,000 Codex JSONL files totaling about
2.3 GiB, with individual files up to roughly 181 MiB. `_read_codex_session_turn`
uses `read_text().splitlines()` on the whole selected file every refresh.

## Required Design

1. Validate session IDs against the exact documented Codex session-ID grammar.
   If current IDs are UUIDs, parse/canonicalize UUIDs rather than maintaining a
   permissive character blacklist.
2. Reject glob metacharacters, prefixes/suffixes, malformed UUIDs, whitespace,
   alternate path spellings, and overlong values before any filesystem walk.
3. Do not interpolate untrusted input into a glob. Resolve exact session identity
   by parsing filenames into a bounded index or by a deterministic exact lookup.
4. Verify the resolved path is a regular file beneath the configured Codex
   sessions root after canonicalization. Refuse symlinks that escape the root.
5. Never select the newest approximate match. Zero matches means unavailable;
   multiple exact-identity matches require a deterministic documented rule and
   must not cross identities.
6. Keep a bounded LRU index/path cache with stat-based invalidation. Do not walk
   all 20,000+ files every polling cycle.
7. Parse incrementally in the long-lived daemon. Cache path, device/inode,
   committed byte offset, pending partial-line bytes, and current turn state.
8. Advance the committed offset only through newline-terminated valid records.
   A partial EOF record must be retained and retried when completed.
9. Detect truncation, inode replacement, path rotation, and missed cache state.
   Resynchronize with a bounded adaptive backward scan that finds the latest
   required turn boundary, then continue incrementally.
10. Bound memory for a single pathological line and total parser state. Report a
    private degraded reason when a record exceeds the limit; do not read the
    entire file as fallback.
11. Preserve prompt, stream, final, completion, internal-automation filtering,
    and source-turn identity semantics exactly for normal sessions.
12. Cache only private source state. No path, inode, offset, session ID, or raw
    record enters public JSON.

Reuse the proven OMP incremental-reader ideas where they fit, but avoid forcing
two source formats into an abstraction that obscures their different record
semantics.

## Implementation Quality Constraints

- Put incremental state in a bounded, explicit data object containing only the
  path identity, committed offset, partial record, and current turn state.
- Keep exact session lookup/indexing separate from JSONL parsing. Neither helper
  should know daemon scheduling policy.
- Never retain an unbounded global path/session map. Cache size and eviction must
  be visible constants/config with deterministic tests.
- Reuse only low-level safe primitives from the OMP reader when semantics match;
  do not force both formats through a callback-heavy generic parser.
- Cold-start/resync is one bounded algorithm, not a chain of whole-file
  fallbacks.
- Keep record interpretation in small pure functions so fixtures test behavior
  without constructing daemon internals.

## Required Tests and Benchmarks

- `*`, `?`, `[abc]`, traversal, separators, prefixes, suffixes, malformed UUIDs,
  Unicode lookalikes, and overlong IDs resolve to no file.
- A valid ID resolves only its exact file in a fake tree containing close IDs and
  newer decoys.
- Symlink escape is rejected.
- A 20,000-file fake/index fixture performs one bounded index build and cached
  subsequent lookups without repeated full walks.
- A large sparse JSONL fixture proves second and later polls read only appended
  bytes, not the whole file.
- A partial final line is invisible until newline completion and is then parsed
  exactly once.
- Truncate, rotate, inode-replace, and daemon-cold-start cases recover the latest
  turn without crossing sessions.
- Huge turns preserve prompt and final, and long working turns continue to
  advance.
- LRU path/parser caches evict predictably and stay within documented memory.

Record cold lookup, warm lookup, cold parse, and incremental poll timings and
bytes read. Avoid assertions tied to one fast developer machine; assert bounded
work and reasonable broad ceilings.

## Acceptance Evidence

- The wildcard probe returns no match.
- Incremental benchmark shows append-sized reads after warm-up.
- Focused Codex/turn tests and full Tendwire suite pass.
- Public sentinel scan contains no session/path/cache internals.

## Non-Goals

- Do not modify Codex's session format.
- Do not index or copy session content into a new external service.
- Do not deploy, merge, or restart services.
