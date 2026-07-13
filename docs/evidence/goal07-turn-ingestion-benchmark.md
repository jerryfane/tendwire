# Goal 07 background turn-ingestion benchmark

## Scope and method

This is synthetic, local evidence from a private memory-backed `TemporaryDirectory`; it did not read or mutate live Tendwire, Herdr, session, socket, database, or connector state. The benchmark generated a schema-v9 SQLite store, two public worker observations, two private scheduler bindings, two current turn revisions, one queued outbox sentinel, an executable private adapter, private FIFO/marker synchronization, and a Unix-domain socket. Generated paths, process IDs, binding identifiers, turn identifiers, content, outbox payloads, and errors are never included in the emitted report.

The real `TendwireDaemon` used the production `TurnIngestionScheduler`, `TendwireDaemonAPI`, and `UnixSocketJSONServer`. Scheduler cadence was 2 seconds, scheduler workers were 4, queue capacity was 64, and the scheduler used the configured 60-second Herdr adapter timeout. The socket server retained its production 8 request workers, 32-request admission bound, and 1 MiB request/response frame bounds. Two generated adapter child processes blocked in kernel FIFO reads. The fixture event backend durably appended two generated `pane.output_matched` rows through the store API before scheduler startup, verified the committed row-count delta, and issued one callback while the two adapter reads were blocked. Subsequent cadence/event scans coalesced work for the already-running bindings.

Every measured `turn.list` and `health.get` call used the original production-bound daemon methods. A fail-closed self-check verified that the measured API object's `_get_turns` callback equaled `daemon.get_turns` and `_get_health` equaled `daemon.get_health`; the socket dispatcher was wrapped only to count active API calls. Thus `turn.list` included production store pagination/projection and `health.get` included production store-status aggregation. `command.submit` used the same real socket/API dispatch with an immediate deterministic dry-run submission hook. Every operation opened a validated Unix socket connection, sent newline-delimited JSON, traversed the production bounded request executor and API sanitization, and read the bounded response frame.

Each operation used 3 warm-ups followed by 21 measured samples. Timing used `perf_counter_ns`; p50 and p95 use nearest-rank, $x_{\lceil pn\rceil}$ after sorting the 21 integer nanosecond samples. Response-byte maxima include the newline frame terminator. The protocol-aligned documented-host evidence gates were `turn.list` p95 $\le 350$ ms and `health.get` p95 $\le 350$ ms, matching the existing 0.35-second fast-read CLI timeout, plus `command.submit` p95 $\le 250$ ms. These timings and gates are evidence for this run on the stated host, not a generic SLA, scaling guarantee, or statistical service-level claim.

## Environment and host load

| Property | Actual value |
|---|---|
| Platform | `Linux-6.12.75+rpt-rpi-2712-aarch64-with-glibc2.41` |
| Kernel | `6.12.75+rpt-rpi-2712` |
| Architecture | `aarch64` |
| Logical CPUs | 4 |
| Python | `3.13.5` |
| SQLite | `3.46.1` |
| Fixture storage | private memory-backed tmpfs `TemporaryDirectory` |
| Timer | `perf_counter_ns` |
| Percentile method | nearest-rank |
| Source import path | `PYTHONPATH=src` |

The corrected final evidence run was not overlapped with project tests or another benchmark. The immediately following host observation reported load averages **7.11 / 9.59 / 11.19** on 4 logical CPUs because the workstation's long-running services remained active. That context is reported rather than normalized away and does not change the measured end-to-end values.

## Exact command and outcome

Run from `/tmp/tendwire-goal07`:

```console
PYTHONPATH=src python3 scripts/turn_ingestion_benchmark.py --workers 8 --blocked-workers 2 --blocked-seconds 5 --warmups 3 --samples 21 --json
```

The exact command exited `0`, emitted exactly one compact JSON object on stdout, and reported `ok: true` and `status: completed`. The report's internal wall time was **7.092835102 seconds**; the harness-observed wall time was **8.04 seconds**.

## Results

### Production-handler real-socket latency while two adapters were blocked

| Operation | Warm-ups | Samples | Minimum | p50 | p95 | Maximum | Max response bytes | Evidence gate | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Production store-backed `turn.list` | 3 | 21 | 31.623977 ms | 49.365592 ms | 70.186477 ms | 79.103229 ms | 2,975 | p95 ≤ 350 ms | passed |
| Production store-backed `health.get` | 3 | 21 | 29.863897 ms | 62.099061 ms | 239.850493 ms | 262.879164 ms | 1,955 | p95 ≤ 350 ms | passed |
| Immediate synthetic `command.submit` | 3 | 21 | 1.777636 ms | 9.886811 ms | 21.233683 ms | 49.430944 ms | 221 | p95 ≤ 250 ms | passed |

The largest measured response frame was 2,975 bytes. The API concurrency probe completed in 52.205824 ms and observed all 8 configured request workers active concurrently while the two source adapters remained blocked. The production-handler self-check passed and the emitted transport mode is `production_store_backed`.

### Adapter, event, scheduler, revision, and outbox evidence

| Observation | Actual result |
|---|---:|
| Configured blocked adapters | 2 |
| Observed maximum adapter concurrency | 2 |
| First-call adapter overlap | 6.071666919 s |
| Source calls before timed requests | 2 |
| Source calls after all timed requests | 2 |
| Final source calls after coalesced cadence/event reruns | 4 |
| Generated committed event rows / callback notifications | 2 / 1 |
| Total event rows after fixture setup | 3 |
| During-block scheduler state | `stale`; active 2; queue 0; refreshed 0; coalesced 3; failed 1; timed out 0; queue full 0 |
| Final scheduler state | `healthy`; active 0; queue 0; refreshed 4; coalesced 7; failed 1; timed out 0; queue full 0 |
| Store schema / persisted bindings | v9 / 2 |
| Revision rows before / after | 4 / 4 |
| Current revision rows after | 2 |
| Duplicate revision groups after | 0 |
| Outbox rows before / after | 1 / 1, exact row preserved |

The unchanged source-call count across every warm-up and measured request proves that production store-backed `turn.list`, production aggregate `health.get`, and synthetic `command.submit` did not initiate adapter reads. Two source calls existed before timing and still existed after all 72 warm-up/measured requests. Cadence/event coalescing later produced one rerun per binding before drain, without adding a revision or changing the outbox row.

The scheduler retained one cumulative failed scan/result counter, and the evidence reports it rather than resetting or suppressing it. Current health was `stale` before any refresh succeeded and recovered to `healthy` after four successful refreshes, even though the lifetime `failed` counter remained 1. The blocked adapters did not time out, queue capacity was never exhausted, and the queue drained to zero.

## Cleanup, bounds, and privacy checks

All fail-closed benchmark checks passed:

- the measured list/health callbacks were the original production daemon methods;
- the 8-worker API probe completed and observed maximum API concurrency 8;
- two adapters overlapped for at least the configured five seconds, with maximum adapter concurrency 2;
- two generated events were durably committed before the fixture backend's single scheduler notification;
- scheduler queue depth was zero during the blocked wave and after drain; coalescing was observed; adapter timeouts and queue-full events remained zero;
- all three measured p95 values met their protocol-aligned documented-host evidence gates;
- schema-v9 revision rows were unchanged, current-revision count stayed 2, and duplicate revision groups stayed zero;
- the single outbox row was exactly unchanged;
- shutdown completed in **38.274462 ms**, within the benchmark's 2-second cleanup bound;
- all 4 generated adapter children were reaped, all benchmark/scheduler/API threads were reaped, the event callback was detached, the event backend stopped, and the Unix socket was removed;
- the temporary directory and marker directory were mode `0700`, the adapter was mode `0700`, the database had no group/other permission bits, and the full temporary fixture was removed after the context exited;
- the recursive generated-value privacy scan covered temporary paths, generated binding/target values, private content and outbox values, and every observed child PID; it passed, and an independent recursive field check found no `error`, `error_type`, or `errors` field in the success report.

## Complete emitted result

```json
{"checks":{"adapter_children_reaped":true,"adapter_worker_bound_observed":true,"api_probe_completed":true,"api_worker_bound_observed":true,"benchmark_threads_reaped":true,"blocked_adapters_overlapped":true,"cached_requests_started_no_source_reads":true,"command_budget_met":true,"daemon_thread_reaped":true,"event_backend_stopped":true,"event_burst_committed_before_notification":true,"event_callback_detached":true,"expected_command_calls":true,"health_budget_met":true,"list_budget_met":true,"no_duplicate_revisions":true,"outbox_rows_unchanged":true,"privacy_scan_passed":true,"private_adapter_executable":true,"private_database_mode":true,"private_marker_directory":true,"private_temporary_directory":true,"production_list_health_handlers_measured":true,"raw_errors_absent":true,"real_unix_socket_removed":true,"revision_rows_unchanged":true,"scheduler_coalescing_observed":true,"scheduler_no_timeouts_or_queue_full":true,"scheduler_queue_drained":true,"shutdown_bounded":true,"single_outbox_row_preserved":true,"temporary_artifacts_removed":true},"cleanup":{"adapter_child_count":4,"adapter_children_alive":0,"benchmark_threads_alive":0,"event_flush_calls":1,"shutdown_bound_ns":2000000000,"shutdown_ns":38274462,"socket_present_after_shutdown":false},"command":"PYTHONPATH=src python3 scripts/turn_ingestion_benchmark.py --workers 8 --blocked-workers 2 --blocked-seconds 5 --warmups 3 --samples 21 --json","environment":{"architecture":"aarch64","fixture_storage":"memory_backed_tmpfs","operating_system":"Linux","percentiles":"nearest_rank","platform":"Linux-6.12.75+rpt-rpi-2712-aarch64-with-glibc2.41","platform_release":"6.12.75+rpt-rpi-2712","python_version":"3.13.5","source_checkout_pythonpath":"src","sqlite_version":"3.46.1","timer":"perf_counter_ns"},"ingestion":{"during_block":{"active":2,"coalesced":3,"failed":1,"queue":0,"queue_full":0,"refreshed":0,"status":"stale","timed_out":0},"event_callback_notifications":1,"event_committed_count":2,"final":{"active":0,"coalesced":7,"failed":1,"queue":0,"queue_full":0,"refreshed":4,"status":"healthy","timed_out":0},"first_call_overlap_ns":6071666919,"observed_max_adapter_concurrency":2,"scheduler_bounds":{"adapter_timeout_seconds":60.0,"max_workers":4,"queue_capacity":64,"refresh_interval_seconds":2.0},"source_calls_after_requests":2,"source_calls_before_requests":2,"source_calls_final":4},"latency_ns":{"command_submit":{"documented_host_budget_met":true,"documented_host_budget_ns":250000000,"max_ns":49430944,"min_ns":1777636,"p50_ns":9886811,"p95_ns":21233683,"response_bytes_max":221,"samples":21,"warmups":3},"health_get":{"documented_host_budget_met":true,"documented_host_budget_ns":350000000,"max_ns":262879164,"min_ns":29863897,"p50_ns":62099061,"p95_ns":239850493,"response_bytes_max":1955,"samples":21,"warmups":3},"turn_list":{"documented_host_budget_met":true,"documented_host_budget_ns":350000000,"max_ns":79103229,"min_ns":31623977,"p50_ns":49365592,"p95_ns":70186477,"response_bytes_max":2975,"samples":21,"warmups":3}},"ok":true,"parameters":{"api_probe_workers":8,"blocked_adapter_workers":2,"blocked_seconds":5.0,"samples_per_operation":21,"warmups_per_operation":3},"schema_version":1,"status":"completed","store":{"bindings":2,"current_revision_rows_after":2,"duplicate_revision_groups_after":0,"event_rows_after":3,"generated_event_rows":2,"outbox_rows_after":1,"outbox_rows_before":1,"revision_rows_after":4,"revision_rows_before":4,"schema_version":9},"transport":{"admission_capacity":32,"dispatches":82,"handler_mode":"production_store_backed","kind":"unix_stream_socket","measured_response_bytes_max":2975,"observed_max_api_concurrency":8,"probe_elapsed_ns":52205824,"request_frame_max_bytes":1048576,"request_workers":8,"response_frame_max_bytes":1048576},"wall_time_ns":7092835102}
```
