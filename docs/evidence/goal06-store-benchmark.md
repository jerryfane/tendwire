# Goal 06 SQLite store lifecycle benchmark

## Scope and method

This is synthetic, local evidence from a private `TemporaryDirectory`; it did not read or mutate live state. The fixture contains 50,000 snapshots for one synthetic host. Every fixture snapshot has a distinct content fingerprint, a fixed 372-byte canonical payload, and a five-minute observation cadence ending at `2026-06-01T00:00:00+00:00`. One durable turn row and one queued connector-outbox row were inserted as migration and maintenance sentinels.

The benchmark uses 3 warm-up operations and 21 measured samples for current-schema initialization, current-schema open, latest read, unchanged save, and changed save. Percentiles are nearest-rank. Retention performs 3 dry-run warm-ups followed by as many real bounded calls as are needed to finish. Timing values are observations from this run, not pass/fail thresholds or service-level claims.

## Environment

| Property | Actual value |
|---|---|
| Platform | `Linux-6.12.75+rpt-rpi-2712-aarch64-with-glibc2.41` |
| Kernel | `6.12.75+rpt-rpi-2712` |
| Architecture | `aarch64` |
| Python | `3.13.5` |
| SQLite | `3.46.1` |
| Timer | `perf_counter_ns` |
| Percentile method | nearest-rank |
| Source import path | `PYTHONPATH=src` |

## Exact command and outcome

Run from `/tmp/tendwire-goal06`:

```console
PYTHONPATH=src python3 scripts/store_benchmark.py --snapshot-rows 50000 --json
```

The command exited `0`, emitted exactly one compact JSON object, reported `ok: true` and `status: completed`, and recorded an internal wall time of **171.568566544 seconds**. The shell-observed wall time was **172.22 seconds**. This is practical for an explicit 50,000-row evidence run on the stated arm64 environment; no timing value was used as an acceptance threshold.

## Results

### Fixture, migration, storage, and row lifecycle

| Observation | Actual result |
|---|---:|
| Fixture snapshots requested / generated | 50,000 / 50,000 |
| Distinct fixture content fingerprints | 50,000 |
| Fixture SQL batches | 50 × 1,000 rows |
| Payload bytes, minimum / maximum | 372 / 372 |
| Durable sentinel rows | 1 turn; 1 outbox |
| v7→v8 migration latency, including timestamp normalization and payload-based legacy-sentinel disambiguation before index creation | 574.538006 ms |
| Migration rows before / after | 50,000 / 50,000 |
| DB-family bytes before / after migration | 33,206,272 / 35,962,880 |
| Rows before / after unchanged-save phase | 50,000 / 50,000 |
| Unchanged-save growth | 0 rows |
| Changed-save growth | 24 rows (3 warm-up + 21 measured) |
| Rows before retention | 50,024 |
| Rows deleted by retention | 45,967 |
| Rows after retention | 4,057 |
| DB-family bytes before / after retention | 35,962,880 / 35,962,880 |
| Live / reclaimable logical bytes after retention | 2,891,776 / 33,005,568 |
| Page count / freelist pages after retention | 8,764 / 8,058 |

The v7→v8 path ran the current timestamp-normalization pass, including its payload read for legacy year-9999 sentinel disambiguation, across the 50,000 regular canonical-UTC fixture snapshots before creating the new indexes; all rows and durable state survived, and the post-migration age-policy result used the expected UTC cutoff. The fixture does not synthesize a quarantined sentinel, so this timing covers the payload-aware migration path without claiming a sentinel conversion case. The unchanged-save phase produced no row growth. The changed-save phase produced exactly the expected 24-row growth. Retention did not run `VACUUM`, so physical DB-family bytes remained 35,962,880 while deleted pages became reclaimable on the freelist.

### Measured operation latency

| Operation | Samples | p50 | p95 | Minimum | Maximum |
|---|---:|---:|---:|---:|---:|
| Current-schema initialization | 21 | 17.331228 ms | 26.886283 ms | 7.783285 ms | 32.487610 ms |
| Current-schema open | 21 | 3.123832 ms | 5.757292 ms | 1.131877 ms | 11.474195 ms |
| Latest snapshot read | 21 | 5.241493 ms | 10.508746 ms | 3.698947 ms | 12.841703 ms |
| Unchanged save | 21 | 9.883742 ms | 15.382088 ms | 8.429066 ms | 15.956424 ms |
| Changed save | 21 | 11.944328 ms | 14.993271 ms | 10.636523 ms | 15.261494 ms |
| Retention call | 460 | 8.352603 ms | 23.275559 ms | 3.394872 ms | 53.223656 ms |

### Ordinary operation comparison: 500 vs. 50,000 fixture rows

The same benchmark was also run from `/tmp/tendwire-goal06` with the smaller supported fixture:

```console
PYTHONPATH=src python3 scripts/store_benchmark.py --snapshot-rows 500 --json
```

That command exited `0` with one JSON object, `ok: true`, and `status: completed`. Its internal wall time was **1.776873053 seconds** and its shell-observed wall time was **9.30 seconds**.

| Ordinary operation | 500 rows p50 | 500 rows p95 | 50,000 rows p50 | 50,000 rows p95 |
|---|---:|---:|---:|---:|
| Current-schema open | 1.090283 ms | 1.651434 ms | 3.123832 ms | 5.757292 ms |
| Latest snapshot read | 3.142739 ms | 5.393714 ms | 5.241493 ms | 10.508746 ms |
| Unchanged save | 7.481781 ms | 11.845526 ms | 9.883742 ms | 15.382088 ms |
| Changed save | 9.777681 ms | 14.720838 ms | 11.944328 ms | 14.993271 ms |

| Correctness / growth fact | 500-row run | 50,000-row run |
|---|---:|---:|
| Fixture rows generated and distinct | 500 | 50,000 |
| Rows preserved by v7→v8 migration | 500 → 500 | 50,000 → 50,000 |
| Unchanged-save row growth | 0 | 0 |
| Changed-save row growth | 24 | 24 |
| Integrity checks before / after migration / after retention | all passed | all passed |
| Foreign-key violations at all three points | 0 | 0 |
| Durable turn / outbox survival | passed / passed | passed / passed |
| Private temporary artifacts removed | `true` | `true` |

Across this observation, a 100× larger fixture kept each ordinary indexed open/read/save p50 and p95 in the same millisecond-scale range rather than increasing proportionally to row count. Write measurements were close between fixture sizes; open and latest read varied more but remained in the low-millisecond range. This comparison is recorded as observed evidence only, not as a performance threshold, scaling guarantee, or claim of statistical significance. The 500-row values are retained from the immediately preceding run because the quarantine-representation migration change did not alter current-schema ordinary operation behavior.

### Bounded retention work

| Observation | Actual result |
|---|---:|
| Policy | 14 days; at most 4,096 snapshots per host including latest |
| Maintenance batch size | 100 |
| Real retention calls | 460 |
| Maximum rows examined in one call | 100 |
| Maximum rows deleted in one call | 100 |
| Terminal `remaining_candidates` | `false` |
| Final maximum rows per host | 4,057 |
| Final rows older than cutoff, excluding each host's latest | 0 |
| Count policy satisfied | `true` |
| Age policy satisfied | `true` |
| Per-call batch bound satisfied | `true` |

The final cutoff was `2026-05-18T00:00:00+00:00`. Retention ended only after no candidates remained. Every call examined and deleted no more than the configured 100-row batch. The final 4,057 rows are below the 4,096 per-host count policy, and there are no over-age rows other than the explicitly protected latest-row case (which was not needed for this final fixture state).

## Query-plan and work evidence

Current-schema open executed five statements, all pragmas: two `PRAGMA user_version` reads plus three other connection pragmas. It executed **0 mutations, 0 DDL statements, 0 journal-mode changes, and 0 vacuum statements**.

SQLite reported these plans:

- Latest read: `SEARCH SNAPSHOTS USING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID=?)`.
- Global age-range candidate seek:
  - `SEARCH CANDIDATE USING COVERING INDEX IDX_SNAPSHOTS_CREATED_HOST_ID (CREATED_AT<?)`;
  - correlated newest-row protection uses `SEARCH NEWEST USING COVERING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID=?)`.
- Per-host count-boundary candidate seek:
  - host enumeration and each boundary lookup use `IDX_SNAPSHOTS_HOST_NEWEST` as a covering index;
  - candidate deletion IDs use `SEARCH CANDIDATE USING COVERING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID=? AND ID<?)`.

All expected indexes were observed and no plan used a temporary B-tree. The count plan reports three `SCAN` nodes over the recursive/materialized `hosts` and `boundaries` CTE results; persistent `snapshots` accesses are covering-index searches rather than full table scans. The age plan has zero unindexed scan nodes and starts with the global `created_at` range seek, while the count plan computes one indexed retention boundary per host before seeking older IDs.

## Preservation and cleanup checks

All fail-closed benchmark invariants passed:

- schema version was 7 before migration and 8 after migration and retention;
- `PRAGMA integrity_check` returned `ok` before migration, after migration, and after retention;
- `PRAGMA foreign_key_check` reported zero violations at all three points;
- all 50,000 fixture rows survived migration;
- the v7→v8 timestamp-normalization path, including its payload read for legacy year-9999 sentinel disambiguation, completed before index creation for all 50,000 regular canonical-UTC fixture rows; retention then used cutoff `2026-05-18T00:00:00+00:00` and found zero older non-latest rows at completion;
- the durable turn and queued outbox rows survived migration and retention with their expected state and private outbox payload intact;
- a latest synthetic snapshot remained readable;
- the temporary directory mode was `0700` and the database had no group/other permission bits;
- current-schema open was non-mutating;
- expected query indexes were used;
- retention terminated within both policies and honored the per-call batch bound;
- the private temporary fixture and its database family were removed after the run.

## Complete emitted result

```json
{"checks":{"after_migration":{"foreign_key_violations":0,"integrity_ok":true,"schema_version":8},"after_retention":{"foreign_key_violations":0,"integrity_ok":true,"schema_version":8},"before":{"foreign_key_violations":0,"integrity_ok":true,"schema_version":7},"changed_save_appended":true,"current_schema_open_nonmutating":true,"durable_outbox_survived":true,"durable_turn_survived":true,"fixture_content_distinct":true,"fixture_row_count_generated":true,"foreign_keys_before_after":true,"integrity_before_after":true,"latest_snapshot_survived":true,"migration_row_count_preserved":true,"private_database_mode":true,"private_temporary_directory":true,"query_indexes_used":true,"retention_age_policy_satisfied":true,"retention_batch_bound_honored":true,"retention_count_policy_satisfied":true,"retention_terminated":true,"schema_migrated_to_current":true,"temporary_artifacts_removed":true,"unchanged_save_deduplicated":true},"command":"PYTHONPATH=src python3 scripts/store_benchmark.py --snapshot-rows 50000 --json","environment":{"architecture":"aarch64","operating_system":"Linux","percentiles":"nearest_rank","platform":"Linux-6.12.75+rpt-rpi-2712-aarch64-with-glibc2.41","platform_release":"6.12.75+rpt-rpi-2712","python_version":"3.13.5","source_checkout_pythonpath":"src","sqlite_version":"3.46.1","timer":"perf_counter_ns"},"fixture":{"distinct_content_fingerprints":50000,"durable_outbox_rows":1,"durable_turn_rows":1,"payload_bytes_fixed":true,"payload_bytes_max":372,"payload_bytes_min":372,"private_database_mode":true,"private_temporary_directory":true,"snapshot_rows":50000,"sql_batches":50},"latency_ns":{"changed_save":{"max_ns":15261494,"min_ns":10636523,"p50_ns":11944328,"p95_ns":14993271,"samples":21,"warmup":3},"current_schema_init":{"max_ns":32487610,"min_ns":7783285,"p50_ns":17331228,"p95_ns":26886283,"samples":21,"warmup":3},"current_schema_open":{"max_ns":11474195,"min_ns":1131877,"p50_ns":3123832,"p95_ns":5757292,"samples":21,"warmup":3},"latest_read":{"max_ns":12841703,"min_ns":3698947,"p50_ns":5241493,"p95_ns":10508746,"samples":21,"warmup":3},"retention_batch":{"max_ns":53223656,"min_ns":3394872,"p50_ns":8352603,"p95_ns":23275559,"samples":460,"warmup":3},"unchanged_save":{"max_ns":15956424,"min_ns":8429066,"p50_ns":9883742,"p95_ns":15382088,"samples":21,"warmup":3}},"migration_v7_to_v8":{"checkpoint_after":{"busy":0,"checkpointed_frames":0,"log_frames":0},"checkpoint_before":{"busy":0,"checkpointed_frames":0,"log_frames":0},"elapsed_ns":574538006,"family_bytes_after":35962880,"family_bytes_before":33206272,"row_count_preserved":true,"rows_after":50000,"rows_before":50000},"ok":true,"parameters":{"fixture_sql_batch_size":1000,"maintenance_batch_size":100,"measurement_samples":21,"observation_cadence_seconds":300,"retention_count_including_latest":4096,"retention_days":14,"snapshot_rows":50000,"warmup_operations":3},"retention":{"batch_bound_honored":true,"calls":460,"deleted_rows":45967,"max_deleted_per_call":100,"max_examined_per_call":100,"terminal_remaining_candidates":false},"retention_policy_state":{"age_policy_satisfied":true,"count_policy_satisfied":true,"cutoff_at":"2026-05-18T00:00:00+00:00","host_row_counts":[{"host_id":"synthetic-benchmark-host","rows":4057}],"max_rows_per_host":4057,"rows_older_than_cutoff_excluding_latest":0,"snapshot_rows":4057},"row_counts":{"after_migration":50000,"after_retention":4057,"after_unchanged_save":50000,"before_retention":50024,"before_unchanged_save":50000,"changed_save_growth":24,"fixture_before_migration":50000,"unchanged_save_growth":0},"schema_version":1,"status":"completed","storage":{"checkpoint_after_retention":{"busy":0,"checkpointed_frames":0,"log_frames":0},"checkpoint_before_retention":{"busy":0,"checkpointed_frames":0,"log_frames":0},"family_bytes_after_migration":35962880,"family_bytes_after_retention":35962880,"family_bytes_before_migration":33206272,"family_bytes_before_retention":35962880,"pages_after":{"freelist_pages":8058,"logical_live_bytes":2891776,"logical_reclaimable_bytes":33005568,"page_count":8764,"page_size_bytes":4096},"pages_before":{"freelist_pages":531,"logical_live_bytes":30965760,"logical_reclaimable_bytes":2174976,"page_count":8091,"page_size_bytes":4096}},"wall_time_ns":171568566544,"work_evidence":{"current_schema_open_statements":{"ddl_statements":0,"journal_mode_statements":0,"mutating_statements":0,"pragma_statements":5,"statements":5,"user_version_statements":2,"vacuum_statements":0},"query_plans":{"latest_read":{"details":["SEARCH SNAPSHOTS USING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID=?)"],"expected_indexes_used":{"idx_snapshots_host_newest":true},"plan_nodes":1,"temporary_btree_nodes":0,"unindexed_scan_nodes":0},"retention_age":{"details":["SEARCH CANDIDATE USING COVERING INDEX IDX_SNAPSHOTS_CREATED_HOST_ID (CREATED_AT<?)","CORRELATED SCALAR SUBQUERY 1","SEARCH NEWEST USING COVERING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID=?)"],"expected_indexes_used":{"idx_snapshots_created_host_id":true,"idx_snapshots_host_newest":true},"plan_nodes":3,"temporary_btree_nodes":0,"unindexed_scan_nodes":0},"retention_count":{"details":["MATERIALIZE BOUNDARIES","CO-ROUTINE HOSTS","SETUP","SEARCH FIRST_HOST USING COVERING INDEX IDX_SNAPSHOTS_HOST_NEWEST","RECURSIVE STEP","SCAN HOSTS","CORRELATED SCALAR SUBQUERY 2","SEARCH NEXT_HOST USING COVERING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID>?)","SCAN HOSTS","CORRELATED SCALAR SUBQUERY 4","SEARCH BOUNDARY USING COVERING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID=?)","SCAN BOUNDARIES","SEARCH CANDIDATE USING COVERING INDEX IDX_SNAPSHOTS_HOST_NEWEST (HOST_ID=? AND ID<?)"],"expected_indexes_used":{"idx_snapshots_host_newest":true},"plan_nodes":13,"temporary_btree_nodes":0,"unindexed_scan_nodes":3}}}}
```
