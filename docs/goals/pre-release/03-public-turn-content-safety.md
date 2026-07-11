# Goal 03: Enforce the Public Turn and Pending Content Contract

## Objective

Prevent generated working progress, tool metadata, and backend-pending data from
publishing private arguments or identifiers while preserving useful, readable
progress.

## Confirmed Defect

`src/tendwire/backends/herdr_turns.py::_omp_public_tool_arg` and
`_omp_tool_snippet` derive public working text from raw tool-call arguments. A
review probe caused an absolute SSH-key path and a Herdr socket path to appear
in a public turn. `src/tendwire/core/turns.py::_public_turn_text` bounds text but
does not make arbitrary raw command/path text safe.

This violates the source-mode boundary even when the enclosing JSON keys are
sanitized: a private value embedded inside a public string is still a leak.

## Required Public Contract

1. Never render raw tool arguments. This includes shell command strings, argv,
   environment values, working directories, stdin, stdout/stderr, URLs with
   credentials, network endpoints, socket paths, and absolute filesystem paths.
2. Never publish raw backend tool-use or pending-decision IDs,
   pane/terminal/session IDs, backend targets, private fingerprints, or Telegram
   identifiers. Public pending objects may use separately derived opaque IDs.
3. Working progress may publish a normalized tool category and a safe summary,
   for example `read: README.md` or `test: pytest`, only when derived from an
   allowlisted structured field.
4. File summaries must be repository-relative and sanitized. If the path cannot
   be proven to be inside an allowed project root, publish only a generic action
   such as `read file`.
5. Shell tools should normally publish an action category (`test`, `build`,
   `git status`) rather than the command. Use a small explicit allowlist; do not
   attempt to redact an arbitrary shell command into safety.
6. Backend pending prompts and choices may expose only user-facing decision text.
   Internal IDs stay private and are mapped to opaque command choices inside
   Tendwire.
7. Apply value sanitization at the final public boundary for snapshots, turns,
   pending payloads, daemon responses, CLI JSON, events/feed items, and connector
   outbox records. Key filtering alone is insufficient.
8. Preserve normal Markdown and user-visible model content where safe. Avoid a
   blanket path regex that destroys URLs, code examples, or ordinary prose.
9. Document the limit honestly: arbitrary secrets copied into free-form model
   prose cannot be identified perfectly. Provider-shaped credentials, known
   private source fields, and generated tool metadata must still be blocked.
10. Keep full raw source data only in the private adapter layer and only as long
    as required. Do not copy it into public projections before sanitization.

Prefer structured safe-progress construction over post-hoc string replacement.
If a tool has no proven-safe summary extractor, publish its name/category only.

## Implementation Quality Constraints

- Build safe progress from structured allowlisted fields. Do not create a large
  regex pipeline that attempts to repair arbitrary shell commands after they
  have entered public text.
- Keep one final public-value sanitizer shared by snapshot, daemon, CLI, feed,
  and outbox builders; endpoint-specific code may only add narrow structured
  extractors.
- Make private/raw and public-safe representations visibly different in names
  or types so unsafe values cannot be passed accidentally.
- Default new tool kinds to generic action-only output. Avoid per-provider
  conditionals scattered through the turn reader.
- Keep the sentinel corpus table-driven and independent from production regex
  implementation details.
- Do not claim perfect arbitrary-secret detection or add heavyweight secret
  scanning as a runtime dependency.

## Required Tests

Build a table-driven sentinel corpus covering:

- absolute and home-relative paths;
- Herdr/user socket paths;
- private IP/port and credential-bearing URLs;
- shell commands, argv, environment assignments, stdout, and stderr;
- provider-shaped API keys, JWT-like tokens, and bearer tokens;
- pane, terminal, backend target, private fingerprint, and tool-use IDs;
- Telegram chat/topic/message identifiers;
- unsafe values nested in pending prompt/choice labels and tool arguments.

Construct provider-shaped values from fragments at runtime so GitHub secret
scanning does not flag the repository itself.

Tests must inspect every public surface listed above and assert both that the
sentinel is absent and that the resulting payload remains useful and valid.
Add positive tests for safe repo-relative filenames, ordinary Markdown, code
blocks, headings, lists, and benign URLs.

## Acceptance Evidence

- The original OMP tool-argument probe exposes no private path/socket value.
- Public JSON and outbox sentinel scans report zero forbidden values.
- Focused turn, pending, boundary, connector-outbox, and daemon tests pass.
- Full Tendwire suite passes without weakening existing sanitizer assertions.

## Non-Goals

- Do not move Telegram rendering into Tendwire.
- Do not publish raw data behind an `experimental` flag.
- Do not add real credentials to tests.
- Do not deploy, merge, or restart services.
