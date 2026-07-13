# Goal 08 Codex session-reader benchmark

## Scope and method

This is synthetic, local evidence from one final run against the current private Codex resolver and reader APIs in `tendwire.backends.herdr_turns`. It did not read or mutate a live `CODEX_HOME`: the script replaced `CODEX_HOME` only for the duration of a private memory-backed `TemporaryDirectory`, restored the prior environment and observer values in `finally`, cleared its private resolver/parser caches, and verified removal of the temporary fixture before reporting success. The success report and this evidence contain only aggregate counts, sizes, timings, limits, booleans, and host context. Generated session identities, turn identity, content, rollout filename, and temporary paths are deliberately absent.

The fixture contained exactly 20,000 regular rollout files under one valid year/month/day hierarchy. One exact rollout was a sparse large JSONL file with a 20 MiB logical prefix, one alignment newline, and a 315-byte valid tail containing a task start and user message. The logical file size before append was 20,971,836 bytes. After the cold parse, the script appended one 261-byte commentary record and polled twice: once for the append and once unchanged.

The script used the production-private `_find_codex_session_file` and `_read_codex_session_turn` entry points. `_CODEX_INDEX_BUILD_OBSERVER` recorded entries visited per index build, and `_CODEX_ISOLATED_READ_OBSERVER` recorded reader bytes per poll. Resolver and parser caches were empty before the wildcard and exact probes. The cold exact lookup therefore includes the complete index build. The warm exact lookup used that cached resolution. “Cold parse” means the parser checkpoint was cold while the exact resolver result was already warm; this distinction prevents the lookup walk from being mislabeled as parsing work.

Timing used `perf_counter_ns`. Each latency is one deterministic scenario observation rather than a percentile distribution. The broad timing ceilings are fail-closed evidence gates for this documented host, not generic service-level objectives. The stable acceptance contract is bounded work: one complete index build, no wildcard or warm index build, 65,536 cold-parse bytes rather than the full sparse logical file, exactly append-sized incremental work, and zero bytes on an unchanged poll.

## Environment and host context

| Property | Observed value |
|---|---|
| Platform | `Linux-6.12.75+rpt-rpi-2712-aarch64-with-glibc2.41` |
| Kernel | `6.12.75+rpt-rpi-2712` |
| Architecture | `aarch64` |
| Logical CPUs | 4 |
| Python | `3.13.5` |
| Fixture storage | private memory-backed tmpfs `TemporaryDirectory` |
| Timer | `perf_counter_ns` |
| Source import path | `PYTHONPATH=src` |
| Load average captured at benchmark start | 10.38 / 11.20 / 9.52 |

The load context is reported rather than normalized away. The benchmark's reported wall time was 1.979177825 seconds. Fixture construction is included in that wall time but not in the cold-lookup interval.

## Exact command and outcome

Run from `/tmp/tendwire-goal07`:

```console
PYTHONPATH=src python3 scripts/codex_session_reader_benchmark.py --json
```

The final documented command exited `0`, wrote exactly one compact JSON object to stdout, and reported `ok: true`, `status: completed`, and report schema version 1. The script also compiled with `python3 -m py_compile scripts/codex_session_reader_benchmark.py`. A focused failure-path smoke check verified that an unsupported `--help` invocation emitted one compact schema-v1 `invalid_arguments` JSON object and exited `2`, rather than emitting prose or succeeding.

## Results

### Host-specific timing evidence

| Operation | Observed | Broad documented-host ceiling | Result |
|---|---:|---:|---|
| Wildcard rejection before filesystem work | 7,870 ns (0.007870 ms) | none | passed |
| Cold exact lookup, including 20,000-file index build | 554,783,518 ns (554.783518 ms) | 30,000,000,000 ns (30 s) | passed |
| Warm exact lookup | 362,262 ns (0.362262 ms) | 1,000,000,000 ns (1 s) | passed |
| Cold parser resynchronization | 1,770,048 ns (1.770048 ms) | 2,000,000,000 ns (2 s) | passed |
| Incremental append poll | 1,088,581 ns (1.088581 ms) | 1,000,000,000 ns (1 s) | passed |
| Unchanged poll | 162,112 ns (0.162112 ms) | 1,000,000,000 ns (1 s) | passed |

These single-run timings establish that the bounded scenarios completed under deliberately broad ceilings on this host. They are not percentile estimates, scaling laws, or latency guarantees for other storage, hosts, loads, or session layouts.

### Deterministic bounded-work evidence

| Observation | Actual | Gate | Result |
|---|---:|---:|---|
| Indexed exact rollout files | 20,000 | exactly 20,000 | passed |
| Index builds across wildcard, cold/warm lookup, and all reads | 1 | exactly 1 | passed |
| Filesystem entries visited by the index build | 20,003 | at most 100,000 | passed |
| Entries retained in index | 20,000 | complete, no overflow | passed |
| Retained index bytes | 2,340,000 | at most 8,388,608 | passed |
| Index builds caused by wildcard probe | 0 | exactly 0 | passed |
| Additional index builds caused by warm lookup | 0 | exactly 0 | passed |
| Cold parse bytes read | 65,536 | at most 65,536 | passed |
| Sparse session logical bytes before append | 20,971,836 | greater than 20 MiB | passed |
| Incremental append / second-poll bytes read | 261 / 261 | exactly equal | passed |
| Unchanged-poll bytes read | 0 | exactly 0 | passed |

The index observer recorded one build visiting 20,003 entries: the 20,000 files plus the year, month, and day directory entries encountered by the bounded hierarchy walk. The exact target resolved to the generated regular rollout and resolved identically on the warm lookup. The wildcard probe returned no match before any index build. The unchanged index-observer count proves neither the warm lookup nor the parser polls repeated the index walk.

The sparse file's 20,971,836-byte logical size did not cause a full read. Cold resynchronization read exactly the 65,536-byte initial tail window. Because the cold fixture tail intentionally ended with only the task start and user record, there were no retained commentary spans to rematerialize. The next poll therefore read exactly the 261 appended bytes, and the following unchanged poll read zero bytes. This directly distinguishes bounded append processing from a repeated full-file read.

## Gates, cleanup, and privacy

All emitted checks passed:

- the wildcard identity produced no match and no filesystem index build;
- the exact generated identity resolved to the exact generated regular rollout;
- the index contained all 20,000 sessions without overflow, visited 20,003 entries within the 100,000-entry production bound, retained 2,340,000 bytes within the 8 MiB production bound, and was built exactly once;
- the warm lookup added no index build;
- cold parse, incremental poll, and unchanged poll returned the expected public reader behavior;
- cold parsing read no more than the 64 KiB initial resynchronization window;
- the second poll read exactly the appended 261 bytes, and the unchanged poll read zero bytes;
- all five documented-host timing ceilings passed;
- the temporary directory was mode `0700`, fixture files were created mode `0600`, and all temporary artifacts were removed;
- the recursive success-report privacy scan rejected any generated temporary path, session identity, turn identity, content, filename, or UUID-shaped string; it passed.

The process returns nonzero on argument errors (`2`) and benchmark/check failures (`1`). Failure reports preserve the compact schema envelope and expose only the exception type, not raw exception text that could contain a private path or identity.

## Limitations

- This is a single synthetic run on memory-backed tmpfs. It does not model cold block-device cache, network filesystems, filesystem contention, or arbitrary real-world directory distributions.
- The index fixture uses one valid date hierarchy. The 20,003 observed visits prove bounded exact work for this required 20,000-file shape, not a universal traversal cost for every distribution permitted by the resolver.
- Parser timing begins after exact resolution is warm. Resolver cost is reported separately as cold and warm lookup.
- The benchmark validates normal sparse-tail recovery, append processing, and unchanged polling. Focused behavior tests, not this performance script, cover malformed records, truncation, replacement, symlinks, ambiguity, partial lines, cache eviction, concurrent publication, and IPC failure modes.
- Timing ceilings are broad host-health gates. The observer-backed visit and byte assertions are the portable regression signal.

## Complete emitted result

```json
{"bounded_work":{"cold_parse_byte_bound":65536,"cold_parse_bytes_read":65536,"filesystem_entries_visited":20003,"filesystem_entry_visit_bound":100000,"generation_entries_visited":20003,"incremental_append_bytes":261,"incremental_poll_bytes_read":261,"index_builds":1,"indexed_sessions":20000,"retained_index_byte_bound":8388608,"retained_index_bytes":2340000,"unchanged_poll_bytes_read":0,"warm_lookup_additional_index_builds":0,"wildcard_index_builds":0},"checks":{"append_sized_second_poll":true,"cold_content_observed":true,"cold_lookup_budget_met":true,"cold_parse_bounded":true,"cold_parse_budget_met":true,"exact_resolution":true,"fixture_file_count_exact":true,"incremental_content_observed":true,"incremental_poll_budget_met":true,"index_build_bounded":true,"index_generation_complete":true,"one_index_build":true,"privacy_scan_passed":true,"private_temporary_directory":true,"sparse_large_fixture":true,"temporary_artifacts_removed":true,"unchanged_poll_budget_met":true,"unchanged_poll_no_read":true,"warm_lookup_budget_met":true,"warm_lookup_no_walk":true,"wildcard_no_match":true,"wildcard_no_walk":true},"command":"PYTHONPATH=src python3 scripts/codex_session_reader_benchmark.py --json","environment":{"architecture":"aarch64","fixture_storage":"memory_backed_tmpfs","load_average_1m_5m_15m":[10.38,11.2,9.52],"logical_cpus":4,"operating_system":"Linux","platform":"Linux-6.12.75+rpt-rpi-2712-aarch64-with-glibc2.41","platform_release":"6.12.75+rpt-rpi-2712","python_version":"3.13.5","source_checkout_pythonpath":"src","timer":"perf_counter_ns"},"fixture":{"append_bytes":261,"file_count":20000,"logical_session_file_bytes_before_append":20971836,"tail_bytes":315},"latency_ns":{"cold_lookup":{"documented_host_budget_met":true,"documented_host_budget_ns":30000000000,"elapsed_ns":554783518},"cold_parse":{"documented_host_budget_met":true,"documented_host_budget_ns":2000000000,"elapsed_ns":1770048},"incremental_poll":{"documented_host_budget_met":true,"documented_host_budget_ns":1000000000,"elapsed_ns":1088581},"unchanged_poll":{"documented_host_budget_met":true,"documented_host_budget_ns":1000000000,"elapsed_ns":162112},"warm_lookup":{"documented_host_budget_met":true,"documented_host_budget_ns":1000000000,"elapsed_ns":362262},"wildcard_probe":7870},"ok":true,"parameters":{"fixture_files":20000,"sparse_prefix_bytes":20971520},"schema_version":1,"status":"completed","wall_time_ns":1979177825}
```
