# Goal 08B SQLite sidecar race recovery evidence

## Scope and isolation

This evidence was recorded on 2026-07-13 from the dedicated isolated checkout at `/tmp/tendwire-goal08b`. The driver built a deterministic, versioned `tendwire-0.1.0-py3-none-any.whl` directly from that checkout, installed it with `pip --no-index --no-deps` into a private temporary virtual environment, re-executed the measured phases with that environment's isolated Python, and required the imported `tendwire` package to resolve beneath the private installation rather than the mutable `/home/smith/tendwire` checkout.

The base Git revision is recorded together with a SHA-256 over every packaged source path, length, and byte sequence and the final wheel SHA-256. `source_revision` is therefore the base revision, not a claim that the worktree was clean; `source_tree_sha256` cryptographically identifies the exact package source used to build the measured wheel, and `wheel_sha256` identifies the installed artifact.

The frozen aggregate records `private_temporary_directory`, `installed_candidate_imported`, `origin_verified`, and `mutable_source_not_imported` as true, plus zero direct Herdr calls and zero outbound network attempts. It does not contain probes for every possible access to live configuration, state, socket, service, or service lifecycle; the isolated-candidate command is an operational instruction, not evidence of the absence of every such access.

## Production paths exercised

The installed candidate exercised:

- the authoritative descriptor-relative SQLite-family preparation transition with real WAL/SHM and rollback-journal files;
- barrier-controlled capture, real SQLite checkpoint/rollback/final-close retirement, and terminal optional absence, without timing sleeps or retry loops;
- a real isolated `TendwireDaemon`, production `TendwireDaemonAPI` callback bindings, `UnixSocketJSONServer`, `DaemonAPIClient`, SQLite store, durable snapshot, and completed schema-v2 turn;
- `snapshot.get`, schema-v2 `turn.list`, and `health.get` over the real private Unix socket during deterministic WAL retirement;
- production Herdres source synchronization/import and client subprocess accounting with `dry_run: true`;
- one settling source pass followed by exactly two valid no-op passes;
- nine production client subprocesses across the three source passes;
- zero direct Herdr calls and zero outbound network attempts; and
- production callback, SQLite-integrity, and private-temporary-directory checks reported as passing.

## Deterministic work and bounds

Portable gates are exact operation counts and cleanup invariants, not host timing thresholds:

| Measurement | Recorded value |
| --- | ---: |
| WAL family iterations | 128 |
| Rollback-journal family iterations | 128 |
| Total bounded family preparations | 256 |
| Real write transactions | 256 |
| WAL checkpoints | 128 |
| Rollbacks | 128 |
| Scheduled WAL disappearances | 128 |
| Scheduled SHM disappearances | 128 |
| Scheduled journal disappearances | 128 |
| Maximum attempts per member | 3 |
| Typed benign-phase failures | 0 |
| Daemon WAL cycles/checkpoints | 64 / 64 |
| Requests per API method | 64 |
| Total successful API requests | 192 |
| API failures | 0 |
| Herdres source passes | 3 |
| Subsequent no-op passes | exactly 2 |
| Production candidate CLI subprocesses | 9 |
| Direct Herdr calls | 0 |
| Outbound network attempts | 0 |

Every WAL and rollback-journal iteration used barriers at the production capture boundary. The writer did not proceed until the selected optional sidecar was captured, and the preparer did not consume the captured state until checkpoint/rollback, final connection close, and verified filesystem retirement completed. The maximum-attempt bound is a fixed transition bound, not a measured retry average.

## API and host timing observations

The measured host was Linux/aarch64 with Python 3.13.5 and SQLite 3.46.1. These latency values are observations from this recorded host, not portable service-level objectives:

| Method | Samples | p50 | p95 | max | Maximum response bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `snapshot.get` | 64 | 36,309,455 ns | 94,875,629 ns | 149,334,187 ns | 598 |
| schema-v2 `turn.list` | 64 | 87,282,482 ns | 222,938,927 ns | 323,836,045 ns | 2,926 |
| `health.get` | 64 | 78,099,793 ns | 172,232,068 ns | 313,801,527 ns | 2,007 |

All three p95 observations were below the evidence-only 350,000,000 ns documented-host budget. Total candidate wall time was 24,513,406,796 ns and candidate process CPU time was 7,784,953,135 ns. Phase wall observations were 1,650,554,137 ns for family churn, 17,016,525,281 ns for daemon/API work, and 5,687,257,211 ns for Herdres source synchronization.

## Resource, cleanup, and privacy proof

Candidate-child resource identities were compared exactly before and after all phases:

| Resource | Before | Peak observed while live | After |
| --- | ---: | ---: | ---: |
| File descriptors | 3 | 10 | 3 |
| Threads | 1 | 11 | 1 |
| Direct children | 0 | 1 | 0 |

The aggregate records balanced managed SQLite connections, a stopped scheduler, removed Unix socket, removed benchmark temporary artifacts, and restored candidate and parent descriptor/thread/direct-child identity sets. Its `privacy_scan_passed` check is true; the compact success report contains aggregate statuses, versions/digests, environment classes, timing observations, and Boolean checks rather than fixture identifiers or content.

## Exact commands and outcomes

Focused driver contract and hostile-cleanup tests:

```sh
cd /tmp/tendwire-goal08b
python3 -m pytest -q tests/test_sqlite_sidecar_race_benchmark.py
```

Outcome: exit `0`; `7 passed in 44.55s`.

Focused benchmark/readiness verification:

```sh
cd /tmp/tendwire-goal08b
python3 -m pytest -q \
  tests/test_release_readiness.py \
  tests/test_sqlite_sidecar_race_benchmark.py
```

Outcome: exit `0`; `15 passed in 37.22s`.

Final installed-candidate benchmark:

```sh
cd /tmp/tendwire-goal08b
python3 scripts/sqlite_sidecar_race_benchmark.py \
  --iterations 128 \
  --daemon-wal-cycles 64 \
  --requests-per-method 64 \
  --herdres-sync-passes 3 \
  --phase-timeout-seconds 120 \
  --json
```

Outcome: exit `0`. The candidate's own measured phase wall time is recorded in the JSON below.

The source invariants exercised by the aggregate are separately covered by the focused harness tests. The aggregate is the authority for this recorded candidate's observable provenance, timing, resource, privacy, and isolation results.

## Current final re-verification

The same packaged source and wheel digests recorded below were re-verified on
2026-07-13 after the final POSIX lock-authority corrections. The exact
same-process regression held a Tendwire `BEGIN IMMEDIATE`, called
`prepare_sqlite_family()`, and required a spawned process using
`PRAGMA busy_timeout=0` plus `BEGIN IMMEDIATE` to report `locked`.

Current isolated outcomes:

- exact lock-authority selection: exit `0`; `21 passed, 230 deselected`;
- `tests/test_local_state_permissions.py`: exit `0`; `133 passed`;
- `tests/test_store.py`: exit `0`; `251 passed`;
- diagnostics, CLI, and daemon regressions: exit `0`; `187 passed`;
- benchmark and release-readiness tests: exit `0`; `15 passed`;
- full Tendwire suite: exit `0`; `1764 passed, 1 skipped`;
- Herdres source-mode fixtures: exit `0`; `113 passed`;
- `python3 -m compileall -q src tests scripts`: exit `0`;
- `git diff --check`: exit `0`.

The installed-candidate command above was also rerun and exited `0` with
`ok: true`, `status: completed`, the same source-tree and wheel SHA-256 values,
256 bounded family preparations, 384 optional disappearances, a maximum of
three attempts per member, 64 successful requests for each API method, three
Herdres source passes with exactly two valid no-op passes, nine candidate CLI
subprocesses, zero direct Herdr calls, and zero external network attempts.
Every aggregate check was true. Candidate resources returned to file
descriptors `3`, threads `1`, and direct children `0`; the isolated socket was
absent after shutdown.

## Frozen compact aggregate JSON

```json
{"accounting":{"candidate_cli_subprocesses":9,"direct_children_after":0,"direct_children_before":0,"direct_children_peak_observed":1,"fd_count_after":3,"fd_count_before":3,"fd_count_peak_observed":10,"parent_direct_children_after":0,"parent_direct_children_before":0,"parent_fd_count_after":13,"parent_fd_count_before":13,"parent_thread_count_after":1,"parent_thread_count_before":1,"socket_present_after":false,"thread_count_after":1,"thread_count_before":1,"thread_count_peak_observed":11},"candidate":{"installation":"private_versioned_wheel","origin_verified":true,"source_revision":"c0ebff7cfba401f6c13da1b58a00abf8ff0b5f36","source_revision_binding":"base_revision_plus_source_tree_sha256","source_tree_sha256":"15b1ca262f6051b191d1587d353c465cc74fd6c6a9d0676eb9348eafef35ff87","version":"0.1.0","wheel_sha256":"7be0f975b0241aaf092a9bba38ace2e3e2efd2f91996f02b2cbcb24b93fac02d"},"checks":{"api_failures_zero":true,"api_request_counts_exact":true,"bounded_family_attempts":true,"daemon_cycle_count_exact":true,"direct_child_set_restored":true,"direct_herdr_calls_zero":true,"duplicate_revisions_zero":true,"external_network_calls_zero":true,"fd_identity_set_restored":true,"fixed_family_counts_completed":true,"installed_candidate_imported":true,"live_direct_child_peak_observed":true,"live_fd_peak_observed":true,"live_thread_peak_observed":true,"managed_connections_balanced":true,"mutable_source_not_imported":true,"no_unexpected_churn_exceptions":true,"noop_state_unchanged":true,"optional_disappearances_observed":true,"parent_direct_child_set_restored":true,"parent_fd_identity_set_restored":true,"parent_thread_identity_set_restored":true,"privacy_scan_passed":true,"private_temporary_directory":true,"production_callbacks_bound":true,"production_client_calls_exact":true,"production_herdres_sync_imported":true,"socket_removed":true,"sqlite_integrity_ok":true,"temporary_artifacts_removed":true,"thread_identity_set_restored":true,"two_noop_syncs_exact":true,"two_noop_syncs_valid":true},"churn":{"bounded_family_preparations":256,"family_iterations":256,"maximum_attempts_per_member":3,"optional_disappearances":384,"rollback_journal":{"checkpoints":0,"iterations":128,"managed_connection_closes":128,"managed_connection_opens":128,"maximum_attempts_per_member":3,"member_phase_observations":768,"mode":"rollback_journal","optional_disappearances":{"journal":128},"preparations":128,"resource_peak_counts":{"direct_children":0,"fds":5,"threads":2},"rollbacks":128,"target_captures":128,"terminal_outcomes":{"absent":384,"invalid":0,"present":128},"typed_codes":{"entry_changed":0,"missing_entry":0,"operation_failed":0,"wrong_owner":0,"wrong_type":0},"write_transactions":128},"unexpected_exceptions":0,"wal":{"checkpoints":128,"iterations":128,"managed_connection_closes":128,"managed_connection_opens":128,"maximum_attempts_per_member":3,"member_phase_observations":771,"mode":"wal","optional_disappearances":{"shm":128,"wal":128},"preparations":128,"resource_peak_counts":{"direct_children":0,"fds":6,"threads":2},"rollbacks":0,"target_captures":128,"terminal_outcomes":{"absent":384,"invalid":0,"present":128},"typed_codes":{"entry_changed":0,"missing_entry":0,"operation_failed":0,"wrong_owner":0,"wrong_type":0},"write_transactions":128}},"daemon":{"api_failures":{},"api_successes":{"health_get":64,"snapshot_get":64,"turn_list":64},"checkpoints":64,"duplicate_revision_groups":0,"integrity_ok":true,"latency_ns":{"health_get":{"documented_host_budget_met":true,"documented_host_budget_ns":350000000,"max_ns":313801527,"min_ns":31718508,"p50_ns":78099793,"p95_ns":172232068,"response_bytes_max":2007,"samples":64},"snapshot_get":{"documented_host_budget_met":true,"documented_host_budget_ns":350000000,"max_ns":149334187,"min_ns":13839581,"p50_ns":36309455,"p95_ns":94875629,"response_bytes_max":598,"samples":64},"turn_list":{"documented_host_budget_met":true,"documented_host_budget_ns":350000000,"max_ns":323836045,"min_ns":37237337,"p50_ns":87282482,"p95_ns":222938927,"response_bytes_max":2926,"samples":64}},"managed_connection_closes":64,"managed_connection_opens":64,"production_callbacks":true,"requests_per_method":64,"resource_peak_counts":{"direct_children":0,"fds":10,"threads":11},"revision_rows":2,"scheduler_refreshes":1,"scheduler_stopped":true,"sidecar_captures":64,"socket_removed_after_shutdown":true,"wal_cycles":64,"write_transactions":64},"environment":{"architecture":"aarch64","platform":"linux","python_version":"3.13.5","sqlite_version":"3.46.1"},"herdres":{"command_sequence_exact":true,"direct_herdr_calls":0,"dry_run":true,"external_network_attempts":0,"mode":"source","noop_passes":2,"noop_passes_valid":2,"noop_work_counts":{"content_pages":0,"created":0,"feed_sent":0,"icon_updated":0,"message_bindings":0,"pinned_status_updated":0,"routing_repaired":0,"sent":0,"turn_updates":0,"updated":0},"production_client_subprocesses":9,"production_sync_import":true,"resource_peak_counts":{"direct_children":1,"fds":8,"threads":10},"settling_changed":false,"settling_ok":true,"settling_passes":1,"state_digest_unchanged":true,"state_unchanged_noop_passes":2,"subprocesses_per_pass":3,"sync_passes":3},"ok":true,"parameters":{"daemon_wal_cycles":64,"herdres_sync_passes":3,"iterations_per_family":128,"maximum_attempts_per_member":3,"phase_timeout_seconds":120.0,"requests_per_method":64},"schema_version":1,"status":"completed","timing_ns":{"children_system_ns":216760999,"children_user_ns":3302452000,"churn_wall":1650554137,"daemon_wall":17016525281,"herdres_wall":5687257211,"process_cpu":7784953135,"self_system_ns":501923000,"self_user_ns":7283079000,"wall":24513406796}}
```

## Limitations

The latency and CPU figures describe this one recorded Raspberry Pi/aarch64 host and are not portable performance guarantees. The operational churn covers benign real SQLite optional-sidecar retirement; deterministic hostile symlink, directory, wrong-owner, different-inode replacement, selected-main disappearance, strict-mode preservation, and descriptor-close behavior remain focused pytest contracts in the core Goal 08B suites. The recorded source-mode run reports `dry_run: true`, one settling pass, and exactly two valid no-op passes; it does not establish the absence of every possible access to configured Herdres state.
