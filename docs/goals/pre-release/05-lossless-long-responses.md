# Goal 05: Preserve Long Responses Without Silent Cutoff

## Objective

Store and deliver complete user prompts and final responses regardless of the
working-card limit, while keeping daemon frames and Telegram messages bounded.

## Confirmed Defect

`src/tendwire/core/turns.py` currently sets `TURN_TEXT_MAX_CHARS = 12000` and
`TURN_STREAM_MAX_CHARS = 4000`. `_public_turn_text` truncates longer values and
adds `[truncated]`. A 20,000-character final was reduced to 11,998 characters
plus the marker. This is permanent data loss before Herdres has a chance to
split the response for Telegram.

The stream limit is reasonable for transient working progress. Applying the same
kind of destructive bound to completed content is not.

## Required Data Contract

1. Persist the full canonical `user_text` and `assistant_final_text`. Never
   overwrite the only copy with a preview or truncation marker.
2. Keep `assistant_stream_text` bounded to recent useful progress. Working
   updates are ephemeral and may use a documented rolling limit.
3. Keep individual daemon responses bounded. Add a versioned, cursor-based
   content API or equivalent deterministic segmentation rather than raising a
   global frame limit until it fails again.
4. A turn listing must distinguish full inline content from a preview. It must
   never label a preview as the complete final. Include an opaque content
   revision, exact character/byte length, segment count, and completion flag
   sufficient for a consumer to fetch all parts.
5. Content segments must concatenate to the exact sanitized canonical text.
   Split on UTF-8/code-point boundaries; do not lose whitespace, list markers,
   code fences, or headings.
6. Segment IDs and cursors must be deterministic, opaque, and stable across
   retries/restarts for the same content revision. They must not embed a DB path,
   source session ID, pane ID, or private fingerprint.
7. If a final is revised by later authoritative observation, produce a new
   content revision and define how consumers replace/update the previous parts.
   Do not append a duplicate full response silently.
8. The neutral connector outbox must represent every deliverable part and track
   acknowledgment independently. A retry must deliver each missing part once,
   in order, without resending acknowledged parts.
9. Herdres remains responsible for Telegram-aware rich-text splitting. Preserve
   headings, paragraphs, lists, copy behavior, and compact styling; final
   responses must not be collapsed merely to fit.
10. When Telegram needs multiple messages, edit the existing working message
    into the first final part where supported, then send ordered continuation
    parts. No part may be silently omitted.
11. Public sanitization from Goal 03 applies before durable public segmentation,
    so a private value is not recoverable through a content page.
12. Add bounded cleanup for superseded content revisions only after no active
    outbox/delivery reference needs them.

Preferred implementation shape: store canonical content separately from the
bounded turn-list projection, expose a paged `turn.content.get`-style daemon
method, and let the connector materialize deterministic outbox parts. An
alternative is acceptable only if it proves the same losslessness, bounded
frames, retry semantics, and compatibility.

## Implementation Quality Constraints

- Keep one canonical public-safe content copy and one deterministic segmenter.
  Do not store multiple independently editable copies of the same final text.
- Separate content storage/transport from Telegram presentation. No Telegram
  size, HTML, topic, or bot logic belongs in Tendwire.
- Keep cursor and revision generation pure and centralized. Consumers must not
  reconstruct identities with duplicated hashing recipes.
- Add the smallest schema/API surface that handles oversized content; avoid a
  generic blob service or attachment framework.
- Keep short-turn compatibility as a thin adapter over the same canonical
  content, not a second ingestion/delivery path.
- Make byte/character limits named and documented at their actual boundary;
  avoid magic slices and nested fallback truncation.

## Compatibility and Migration

- Existing short turns should retain the current convenient inline fields and
  require no extra round trip.
- Existing rows containing `[truncated]` cannot be reconstructed from Tendwire
  alone. Do not pretend migration recovered them. Re-observe from a still
  available source session when safe; otherwise mark historical content as
  known incomplete.
- Add schema/version negotiation so an older Herdres fails clearly rather than
  publishing only a preview.
- Do not include raw source file offsets or paths in the public continuation
  contract.

## Required Tests

- Boundary cases at 3,999/4,000/4,001 and
  11,999/12,000/12,001 characters.
- Finals of 20,000 characters and larger than the daemon's current 1 MiB frame
  concatenate exactly after page retrieval.
- Multibyte Unicode, combining characters, long unbroken words, Markdown code
  fences, headings, nested-looking lists, and blank-line spacing survive.
- A forced failure after part N retries only the unacknowledged parts.
- Two forced source syncs after completion send zero duplicate parts.
- A revised final updates/replaces according to the documented revision policy
  and does not leave contradictory duplicate responses.
- Existing short-message JSON and Herdres rendering remain compatible.
- Forbidden-value scans cover every page and outbox part.

## Acceptance Evidence

- The 20,000-character probe round-trips byte-for-byte after sanitization.
- No code path appends `[truncated]` to the only stored final copy.
- Full Tendwire and relevant Herdres suites pass.
- A hermetic connector smoke proves ordered, exactly-once multipart delivery.

## Non-Goals

- Do not make every working update unbounded.
- Do not move Telegram formatting into Tendwire.
- Do not deploy, merge, or restart services.
