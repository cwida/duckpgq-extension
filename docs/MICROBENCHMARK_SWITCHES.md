# Microbenchmark switches (W19)

Terminology: the paper calls these microbenchmarks (decided 26 Oct); earlier notes say microbenchmark.

26 October 2026. An inventory of every mechanism in the path-finding operator and the CSR index that changes
measured query time, whether it can be switched at runtime, and the sweep that measures each one. The harness
side is `scripts/pathfinding_benchmark.py microbench` (section 4); the results feed the microbenchmark subsection of the
evaluation. File references are to the tree at the time of writing.

## 1. The microbenchmarks the paper needs

The evaluation claims rest on a handful of design decisions. Each needs one row in a microbenchmark table showing
what the decision is worth, measured on the shapes where it applies (SF10 and SF100, 32 threads, 3 repeats,
600 s timeout, both routes unless stated).

| # | Decision | Switch (config name in the harness) | Shapes where it shows |
|---|---|---|---|
| A1 | Per-source frontier kernel for few sources, MS-BFS lanes otherwise | `no_source_grouping`, `no_few_sources_rule`, `source_group_ratio_1/16` | random pairs (lanes win), same destination and sources x all (per-source wins) |
| A2 | Batch scheduling: adaptive worker groups vs one event per batch, fixed shapes G x W, pooled vs own threads | `scheduling` group: `no_grouped_batches`, `adaptive`, `adaptive_os4/os32`, `sched_g<G>_w<W>` | random pairs 2048 / 16384 / 65536 at SF10, SF100, SF300 (the crossover figure) |
| A3 | Stop a source's search once its destinations are found | `no_early_termination` | random pairs with few sources, same destination |
| A4 | Sparse expansion by row lookup vs partition sweep | `dense_frontier`, `sparse_frontier` | sources x all (frontier grows to the whole graph), same destination (small frontiers) |
| A5 | Reverse orientation for many sources to few destinations | `no_reverse_orientation`, `reverse_index`, `reverse_index_forward_only` | same destination, index route; directed graph needs the reverse side |
| A6 | Reach mode instead of a pair list for sources x all | `pair_mode` | sources x all |
| A7 | Snapshot cache over the index | `no_snapshot_cache` | every shape on the index route; the difference is the overlay packing per query, visible once the change log is non-empty (combine with the update workload) |
| A8 | Pair deduplication | `dedupe_pairs` | random pairs (no duplicates: pure overhead) and a duplicated pair table (to add) |
| A9 | Partition count | `light_partitions_4` | all shapes; index route rebuilds the index with the new count |
| A10 | Thread scaling | `threads_1/2/4/8/16` | random pairs 65536, sources x all 1024 |
| A11 | MS-BFS lane width | build-time `cmake -DDUCKPGQ_LANE_LIMIT=64|128|256|1024` (separate builds, section 3; `DUCKPGQ_EXTENSION` picks the build) | random pairs; 256 lanes measured 27 Oct: same time and misses as 512 at SF10 and SF100 |
| A12 | Fold ratio and overlay size | `update-run --fold-ratio`, configs `index_auto` vs `index_nofold` (existing) | update workload |

## 2. Inventory

Options are `SET experimental_path_finding_operator_<name>` unless written in full. "Always on" means the code path
has no switch; the right-hand column says what a microbenchmark would need.

### 2.1 Switchable today

| Mechanism | Control | Default | Notes |
|---|---|---|---|
| Source grouping (per-source frontier kernel vs MS-BFS) | `source_group_ratio` | 4 (<= 0 off) | pair_count >= ratio x distinct sources selects the per-source kernel; SHORTEST_PATH and REACH always use it |
| Few-sources rule | `few_sources_limit` (new, 26 Oct) | 16 (0 off) | at most this many distinct sources always uses the per-source kernel |
| Target early termination | `target_early_termination` (new) | true | a group stops once every destination is reached; `SourceGroupTargets` set/bitmap switch at 64 x expected > V stays hard-coded |
| Sparse vs dense frontier expansion | `sparse_frontier_divisor` (new) | 64 (0 always sweeps; a huge value always looks up rows) | per-source kernel only; needs row lookup (not on streaming segments) |
| Snapshot cache | `snapshot_cache` (new) | true | `CSRIndex::BuildSnapshot` skips both the hit test and the store when false |
| Grouped batches | `grouped_batches` | true | The adaptive batch scheduler (W31): worker groups with pooled working sets, window grown on measured throughput; false runs one event per batch. `grouped_batches_budget` (512 MB) separates the cache-resident regime (start wide, own threads) from the memory-bound one (ramp from 4, pooled tasks); `batch_oversubscription` (0 = by regime) and `max_concurrent_batches` / `threads_per_batch` fix the shape for the `sched_g<G>_w<W>` configurations. The `run` subcommand's `--grouped-batches` flag defaulted to false until 26 Oct, so the 21 Oct DuckDB-only sweep (`summary_*_comparison` files) ran without grouped batches |
| Workers per batch / concurrent batches | `threads_per_batch`, `max_concurrent_batches` | 0 (derive) | |
| Reverse orientation | `reverse_orientation_ratio` | 4 (<= 0 off) | ITERATIVE_LENGTH only; feasible with symmetric edges, an index `WITH (reverse = true)`, or the legacy CSR |
| Reverse side of the index | `CREATE INDEX ... WITH (reverse = true)` | off | storage format 2 |
| Undirected aliasing | `CREATE INDEX ... WITH (undirected = true)`, `-[]-` in MATCH, cache key suffix `|undirected` | off | reverse search over forward partitions |
| Pair deduplication | `deduplicate_pairs` | false | ITERATIVE_LENGTH without an index snapshot |
| Reach mode planning | `reach`, `reach_filtered` | true, false | MATCH only; the harness calls `reachoperator` directly |
| Partition count | `light_partition_multiplier` (and `SET threads`) | 1 | partitions = threads x (1 + max(1, multiplier)), width <= 65535 vertices; the index freezes the count at CREATE INDEX |
| Fold ratio | `duckpgq_csr_index_fold_ratio` | 0.1 (0 off) | manual `duckpgq_csr_index_fold(table, index)` |
| Push/pull, bidirectional kernels | marker function name (`pushpulliterativelengthoperator`, `bidirectionaliterativelengthoperator`), `push_pull_frontier_gate` (2), `build_reverse_csr` | legacy global-CSR input only | not reachable from MATCH; harness modes `pushpull_operator`, `bidirectional_operator` |
| Heavy/light partition split | `heavy_partition_fraction` (0.75), `light_partition_multiplier` | legacy CSR build only | not exercised by the buffered or index paths |
| Thread count | `SET threads` | | feeds partition count, worker counts, Finalize parallelism |
| Instrumentation | `benchmark`, `benchmark_lane_activity`, `benchmark_prefix` | false | keep off in microbenchmarks |

### 2.2 Hard-coded, kept that way

These are structural constants whose microbenchmark would not inform a design decision in the paper, or whose effect is
covered by a switch above. They stay as they are, listed here so the inventory is complete.

| Constant | Value | Where |
|---|---|---|
| MS-BFS lane width `LANE_LIMIT` | 512 | `utils/duckpgq_utils.hpp`; overridable at build time since 26 Oct (`-DDUCKPGQ_LANE_LIMIT`) |
| Pair batch | one vector, `STANDARD_VECTOR_SIZE` = 2048 pairs (four lane waves) | operator sink |
| `EDGES_PER_WORKER` | 65536 | minimum work per worker in `ChooseWorkerCount` |
| `FRONTIER_SLICE` | 256 | work unit of the sparse expansion |
| `SourceGroupTargets` set-to-bitmap switch | 64 x expected targets > V | |
| Single-source zero-copy path | always when every pair has the same source | `TryGetExactSingleSearchSource` |
| Row-bucket index for sparse row lookup | always built, about two rows per bucket | `compressed_sparse_row.hpp` |
| Dictionary form (dense bitmap / identity / sorted keys) | dense when range <= 16 x edges + 4096 | `CSRIndex::BuildDictionary` |
| `MAX_RETAINED_EPOCHS` | 2 | memory after a fold, not query time |
| Change groups, delta id pre-resolution | always | snapshot visibility |
| `REACH_SLICE` | 65536 vertices per output slice | reach source |
| `EDGE_SLICE_SIZE` (index build and companion packing) | 65536 | CREATE INDEX time |
| Partition build order (largest first), `RADIX_BITS` = 11 | | per-query build only |
| Parallel Finalize (pair analysis) | threads = `SET threads` | no serial switch; `threads_1` covers it |
| Dead option `task_size` | registered, never read | to be removed before the freeze |

### 2.3 Switches that could still be added

Only if the writing needs the number. Each is a one-line gate.

- `bidirectional` over the index (W28 step 4) if the random-pairs microbenchmark wants a second kernel to compare with.
- A lengths-only index (no row ids, about 8 B/edge less) for the storage discussion; today the index always keeps
  row ids for path reconstruction.
- A serial pair-analysis switch, if the Finalize share at 65536 pairs needs isolating beyond `threads_1`.
- Moving the snapshot-cache hit test before the visibility pass (an optimisation, not a switch): today a hit
  still pays the per-delta and per-tombstone `CanFetch` loops.

## 3. Lane width builds

`LANE_LIMIT` is a compile-time constant (the bitsets are `std::bitset<LANE_LIMIT>`). The CMake option
`DUCKPGQ_LANE_LIMIT` passes it as a define:

```
GEN=ninja EXT_FLAGS="-DDUCKPGQ_LANE_LIMIT=256" make release
```

builds into the same `build/release` directory, so lane-width variants are separate builds run one after another
(copy `build/release/extension/duckpgq/duckpgq.duckdb_extension` aside and point `DUCKPGQ_EXTENSION` at it, or run
the microbenchmark between builds). The legacy scalar UDFs share the constant.

## 4. The harness

```
python3 scripts/pathfinding_benchmark.py microbench --scale-factor 10 --threads 32 --repeats 3 --timeout-s 600 \
    [--routes build index] [--configs all | kernel | index | reach | threads | <names>] \
    [--shapes random same_dst multi_source] [--pair-counts 1 128 1024 65536] [--source-counts 1 128 1024]
python3 scripts/pathfinding_benchmark.py microbench-table data/ldbc-pathfinding/results/sf10/microbench_*.csv
```

One configuration is a set of `SET` overrides, an optional `CREATE INDEX WITH` clause (the index route creates and
keeps a separate index per clause, `pathfinding_benchmark_csr_reverse_true`), and the pair-list form of the
sources x all shape (`pair_mode`). Every point runs in a fresh process with the timeout; a timed-out point skips
the larger points of its shape for that configuration. Rows carry `config` and `settings`, the result CSV is
`microbench_<sf>_threads<t>_<ns>.csv`, and the table prints each configuration as mean seconds with its ratio to
the baseline. The driver checks that every configuration returns the same reachable count and length sum per
point.

Configurations (groups in `MICROBENCH_GROUPS`): `kernel` = baseline, no_grouped_batches, dedupe_pairs,
threads_per_batch_4, no_source_grouping, no_few_sources_rule, source_group_ratio_1, source_group_ratio_16,
no_early_termination, dense_frontier, sparse_frontier, light_partitions_4; `index` = baseline, no_snapshot_cache,
reverse_index, reverse_index_forward_only, no_reverse_orientation; `reach` = baseline, pair_mode; `threads` =
baseline, threads_1/2/4/8/16.

## 5. Plan

1. Smoke at SF10 (1 repeat, pair counts 1/128/1024, sources 1/128) to check every configuration runs and agrees.
2. Full microbenchmark at SF10 and SF100 on the current build, 3 repeats: `all` configurations, all three shapes.
3. Lane-width builds (64, 128, 256, 1024) on random pairs at SF10.
4. Freeze (tag `eval-freeze-rc1`), then the frozen rerun includes the microbenchmark as a `run_all.sh microbench` step.
