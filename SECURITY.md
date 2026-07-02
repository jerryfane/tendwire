# Security

Tendwire is designed for local-first operation on a trusted developer machine.
It is not a remote multi-tenant control plane and should not be exposed directly
to untrusted networks.

## Local Trust Boundary

Tendwire observes a local Herdr session through either conservative CLI probes or
the explicitly enabled Herdr socket/event backend. Private backend identifiers
may exist in memory or in the local Tendwire store so Tendwire can route commands,
but public JSON must not expose raw `pane_id`, `terminal_id`, `backend_target`,
raw target values, private fingerprints, Telegram chat/topic/message IDs, socket
paths, argv/env/stdout/stderr, tokens, or secrets.

The connector outbox is a neutral boundary. It stores public connector jobs and
public-safe delivery state; concrete Telegram delivery, topic routing, retries,
and rate limits stay in Herdres or another connector process.

## Secrets

Do not commit local environment files, SQLite stores, sockets, logs, caches, or
state dumps. Keep bot tokens, API keys, Herdr socket paths, local usernames, and
machine-specific paths out of issues and pull requests.

Use `.env.example` as a template only. Production values belong in local service
environment files or supervisor configuration.

## Reporting

For public reports, include the command, version or commit, sanitized JSON, and
expected versus actual behavior. Redact local paths, tokens, raw pane or terminal
identifiers, chat/topic/message IDs, and any command output that may include
private terminal text.
