# Tendwire

Tendwire is a **local-first control plane for terminal-based agents**. It watches
spaces and workers managed by tools such as Herdr, summarizes their state,
identifies items that may need human attention, and exposes a neutral,
device-independent API that clients like Telegram, Hermes, iOS, Siri, macOS,
Vision Pro, and AR glasses can consume.

## Relationship to Herdres

[Herdres](https://github.com/plotarmordev/herdres) remains the Telegram connector
and UI. It may later consume Tendwire as a neutral state source, but Tendwire
core is intentionally **not** Telegram, Herdres delivery, or connector-specific
routing code. Tendwire keeps its models and snapshot output free of bot tokens,
chat IDs, Telegram topics, message IDs, and delivery state.

## Running Tendwire

Tendwire installs a console script named `tendwire`:

```bash
tendwire snapshot --json
```

You can also run the CLI module directly:

```bash
python -m tendwire.cli snapshot --json
```

Both print a neutral JSON snapshot to stdout and exit successfully, even when no
Herdr data is available.

## Neutral snapshot shape

The `snapshot --json` output uses this device-neutral contract:

```json
{
  "host_id": "myhostname",
  "updated_at": "2026-06-27T16:18:34+00:00",
  "spaces": [],
  "workers": [],
  "attention": []
}
```

Top-level keys:

- `host_id` — string identifying the host.
- `updated_at` — ISO-8601 UTC timestamp string.
- `spaces` — list of neutral space observations.
- `workers` — list of neutral worker observations.
- `attention` — list of attention signals derived from the snapshot.

## Milestone 1 non-goals

This milestone intentionally does **not** include:

- A Telegram connector or UI implementation.
- Modifications to Herdres.
- A persistent daemon or network sync.
- Connector-specific routing or delivery state inside core models.

The focus is the package skeleton, the neutral snapshot contract, and the
`tendwire snapshot --json` command.
