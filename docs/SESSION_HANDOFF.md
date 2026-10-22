# Session handoff (written 21 Oct 2026)

Read this first when continuing the path-finding paper work in a fresh Claude Code session, on any machine.
The durable state lives in git: this file, `docs/PATHFINDING_PAPER_PLAN.md` (work items with Status column,
schedule, progress log in section 11, sweep recipe in 10b) and `docs/CSR_INDEX_DESIGN.md` (index design and
measurements). The private memory directory of the laptop session is not needed; its content is summarised here.

## Where things stand

Branch `pathfindingoperator-forward-port` (DuckDB v2.0 submodule `45f4fdf3`, `.github/duckdb-version` must match).
Paper: VLDB 2027 rolling, 1 February 2027 round (abstract 25 January), complete draft 1 January. Thesis: a loadable
extension gives a relational engine graph-native path finding, including an MVCC-consistent CSR index.

Done (all committed, suite 82 test files green): W1-W6, W8-W11, W14 (prototype persistence and per-connection
cache removed), W26 (point-query kernel: source-grouped frontier kernel on index snapshots, depth bound, bucket
row index), W7 step 1 (shortest paths over the index through the operator), harness index modes.

Open P1 engineering, in recommended order:
1. W15 remaining index tests: restart under load, concurrent writers with a reader mid-query, fold during a long
   snapshot, drop while in use.
2. W7 step 2: paths on the build-per-query route (edge row id through the endpoint STRUCT(src, dst, id) and the
   radix build; `shortestpathoperator` already has the buffered overloads).
3. W12/W13: cost-based index-versus-build choice and the cost model, after the first sweep.
Then W16 (update workload in the harness), W24/W25 if time.

User side: push the branch, DuckDB-only sweep on the r7a.8xlarge (plan 10b), W17 LDBC queries for Kuzu/Neo4j,
W18 Neo4j Enterprise request, W22 outline.

## Working conventions

- No `Co-Authored-By` or "Generated with" trailers in commits (global rule). Plain commit messages.
- Every test file sets `SET active_grammar_extensions = ['duckpgq']` per connection, after `restart`, inside
  `concurrentloop`, and for named connections.
- `make format-fix` reformats ~20 untouched files on this branch; format only edited files with
  `duckdb/.cache/prepare-venv/bin/clang-format -i <files>` (the venv is created by `make format-check` once).
- Build `GEN=ninja make release`; tests `build/release/test/unittest 'test/sql/*'` (full) or
  `'test/sql/path_finding/*'`.
- CLI for ad-hoc runs: `build/release/duckdb -unsigned -no-agent <db>` and `LOAD` the built extension.
- Timing databases used on the laptop (`/tmp/pf_cold.db`, 2M vertices / 8M edges, tables V/E, graph g, index
  `e_csr`) do not exist elsewhere; `docs/experiments/cold_start/` has the scripts to recreate the graph.
- The harness `scripts/pathfinding_benchmark.py` needs the LDBC generator extension from `~/git/ldbc-data-gen`
  for `prepare`; a hand-made tiny database works for smoke tests (see plan section 10b and the harness memory
  note: tables `person(id)`, `person_knows_person(person1id, person2id)`, `benchmark_metadata(key, value)` at
  `data/ldbc-pathfinding/db/ldbc_sf<sf>.duckdb`).

## Pitfalls already met

- `Event::InsertEvent` moves the parents: insert one completion event and add the rest as dependencies.
- Barriers across ExecutorTasks must be abortable (`MakeQueryBarrier`, `BarrierAbortOnUnwind`).
- Never evaluate row visibility at a past bound: DuckDB garbage-collects row versions; the fold stamps epochs with
  the generation and compacts with a live transaction.
- A writing transaction (`transaction.ChangesMade()`) gets a private snapshot; never publish its epoch.
- Scalar functions that throw need `SetFallible()`; the pre-optimizer sees literals as `BoundCastExpression`, fold
  them with `ExpressionExecutor::TryEvaluateScalar`.
- The eager CSR build used `threads_per_batch = 1` to avoid a scheduler deadlock; every kernel scheduler honours
  `GetPathFindingThreadsPerBatch`.
- Any pair with a NULL endpoint used to disable source grouping (288 s single-source query); fixed in `5455ec18`.
- DuckDB v2: `ListVector::GetEntry` is deprecated (use `GetChildMutable`); uncommitted rows have
  transaction-local row ids (~2^55), so tests compare against `rowid` rather than literals.
- zsh: `--include=` globs and `rm -f glob` abort the command; `cd` in one parallel shell call changes the cwd for
  the others, use absolute paths.

## 21 Oct EC2 session (m7a.8xlarge, eu-west-1)

- Sweep done for SF1, 3, 10, 30, 100, 300 (plan 10b plus larger factors); summary tables in `~/sweep/collected*.md`
  on the instance, raw CSVs in `data/ldbc-pathfinding/results/sf*/`. Progress log entry in the plan has the findings.
- Uncommitted working-tree changes (review before committing): pair-column fix in `logical_path_finding_operator.cpp`,
  `physical_path_finding_operator.{cpp,hpp}`, `path_finding_optimizer_rule.cpp` (legacy route now passes the
  function expression); new test `test/sql/path_finding/path_finding_pair_columns.test`; harness
  `index_query_sql` key-pair materialization, `index_operator_warm` mode, verifier skip of `index_build` rows.
  Full suite 83/83 after the changes.
- Index query times in the sf1-sf300 sweep CSVs are cold-process numbers from before the parallel load: the first
  query paid the serial index deserialization (`CSRIndex::LoadFromStorage`, ~42M edges/s, 1.15 s at SF300). The
  load is parallel since commit "Load a persisted CSR index in parallel" (90 ms at SF300, 32 threads). Use
  `index_operator_warm` for resident-index numbers; the plan's progress log has the re-measured SF300 values.
- EBS on this instance is 250 MB/s (measured). The generator repo is `github.com/Dtenwolde/ldbc-data-gen`; `prepare`
  needs a temporary `third_party -> ~/git/ldbc-data-gen/third_party` symlink in the repo root (dictionary paths
  are cwd-relative); remove it afterwards. AWS CLI credentials on the box belong to another account.
- zsh pitfall: `pkill -f '<pattern>'` from a tool shell matches the shell itself; `set -- $var` does not split words.
- W16 harness: `update-run`, `update-concurrent` (driver `scripts/tools/update_concurrent_driver.cpp`, compiled by
  the harness against build/release/src/libduckdb.so), `update-kuzu` (pip `kuzu` 0.11.3 installed for the user).
  Collectors `~/sweep/collect_update.py`, `~/sweep/collect_concurrent.py`. Since 22 Oct the kernels run as event
  chains (`phased_kernel_event.hpp`), there are no barriers across tasks and the driver needs no worker cap.
- Kernel pattern: a `PhasedKernel` has Start / RunPhase(worker) / AdvancePhase / Finished; serial work goes into
  Start and AdvancePhase (they run on one thread between phases), parallel work into RunPhase; never block inside a
  task.
- The CSR index now stamps change groups and caches the last snapshot (commits f585926a, 3c5b8b78); the design doc's
  "dictionary is a prefix of every later version" is not what the code does (rebuilt in key order at a fold).
