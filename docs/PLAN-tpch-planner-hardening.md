# TPC-H Planner Hardening Plan

This document is the working plan for making the DuckCluster planner and fragment generator
handle all **22 TPC-H queries** before running performance benchmarks.

**Benchmarking is blocked until this plan is complete.** The Java harness stays as-is;
Python is used only for post-run analysis. See [BENCHMARK.md](BENCHMARK.md) for the
benchmark checklist (fill in after planner work).

**Design principle:** There is **one planner** — one code path from SQL → planned fragments →
merge. No user-facing modes, no “simple vs advanced” switches. Every query goes through the
same parse → classify → analyze → rewrite → execute → merge pipeline. Gaps are fixed in that
pipeline, not papered over with alternate behaviour.

---

## Current state (SF0.01 smoke run, 2026-07-09)

| Engine | Supported queries (17) | Result |
|--------|------------------------|--------|
| `duckdb-single` | 17/17 | OK |
| `duckcluster` | 0/17 | Planning, execution, or merge errors |

Root causes are documented in the error taxonomy below. Integration tests did not catch this
because they exercise a single `events` table with simple `SUM(score)` / `AVG(score)` patterns,
not TPC-H grammar.

---

## Pipeline (single code path)

```
SQL
 └─► Calcite parse
      └─► classifyTables (driving table + broadcast tables)
           └─► detectMergeStrategy
                └─► QueryAnalysisExtractor (group keys + aggregates + output columns)
                     └─► FragmentSqlGenerator (shard-qualified FROM + aggregate rewrite)
                          └─► Workers execute fragments
                               └─► MergeSqlBuilder + coordinator DuckDB
```

All phases below extend this **one** pipeline. Nothing branches to a separate “pass-through”
or “rewrite” mode.

---

## Error taxonomy (mapped to TPC-H queries)

| ID | Failure pattern | Queries | Layer |
|----|-----------------|---------|-------|
| E1 | `Expected column identifier but found: <expr>` | Q01, Q03, Q05, Q06, Q10, Q12, Q19 | `QueryAnalysisExtractor` |
| E2 | `No tables found in FROM clause` | Q07, Q08, Q09, Q13 | `classifyTables` / `rewriteFrom` |
| E3 | Parser error (reserved alias `value`, etc.) | Q11 | Calcite parser config |
| E4 | Multi-aggregate arithmetic in one SELECT item | Q14 | Analysis + fragment + merge |
| E5 | Worker fragment failure on multi-table SQL | Q04, Q16, Q17, Q18 | Fragment SQL + execution |
| E6 | Merge failure after partial execution | Q14 | `MergeSqlBuilder` / temp table |
| E7 | Correlated / semi-join subqueries in WHERE | Q02, Q04, Q15, Q17, Q18, Q20, Q21, Q22 | Predicate planning + execution |
| E8 | `COUNT(DISTINCT …)` | Q16 | Aggregate decomposition + merge |

**Target:** all 22 queries pass correctness on small-scale TPC-H data (SF0.01 or equivalent).

---

## Phase 0 — TPC-H correctness gate (small scale)

**Goal:** Every subsequent fix is verified against real TPC-H SQL on tiny data, not only the
`events` demo table.

### Deliverables

- [x] `tests/integration/queries/tpch/` — one file per query (`q01.sql` … `q22.sql`), symlinked
      or copied from `benchmark/src/main/resources/queries/`
- [x] Integration tests that start a cluster with SF0.01 benchmark data
      (`benchmark/data/sf0.01/` or generated in CI)
- [x] Correctness: DuckCluster result == single-node `baseline.duckdb` (same approach as
      existing `duckcluster/baseline.py`)
- [x] `CalciteQueryPlannerTest` extended with `tpchCatalog` fixture:
      `lineitem`×6, `orders`×6, dimension tables ×1 shard each
- [ ] Track per-query status in this doc (checkboxes at bottom)

### How to run locally

```bash
./scripts/benchmark-datagen.sh 0.01          # once
./scripts/run-integration-tests.sh -k tpch   # as tpch tests land
```

### Exit criteria

- CI can run TPC-H correctness for any query marked done in the checklist
- Failures produce actionable messages (planning vs worker vs merge)

---

## Phase 1 — Expression-aware aggregate analysis

**Fixes:** E1  
**Unlocks planning for:** Q01, Q03, Q05, Q06, Q10, Q12, Q19 (execution may still need later phases)

### Problem

`QueryAnalysisExtractor.extractInputColumn()` calls `columnName()`, which only accepts bare
`SqlIdentifier` nodes. TPC-H uses `SUM(l_extendedprice * (1 - l_discount))`, `SUM(CASE WHEN …)`,
etc.

`FragmentSqlGenerator.rewriteAggregates()` already preserves full operand `SqlNode`s for `SUM` /
`COUNT` — the failure happens earlier, during analysis.

### Approach (single pipeline)

1. Extend `AggregateSpec`:
   - `inputColumn` becomes **nullable** (`String`, may be `null`)
   - Add optional `SqlNode inputExpression` (or store serialized expression for merge metadata)
   - When operand is a bare column → set `inputColumn`
   - When operand is an expression → `inputColumn = null`, `inputExpression = operand`

2. Update `QueryAnalysisExtractor`:
   - `extractInputColumn()` never throws on expressions
   - `outputName()` prefers `AS` alias; fallback `__dc_agg_<n>` naming
   - GROUP BY keys remain simple identifiers (true for all TPC-H GROUP BY columns)

3. `FragmentSqlGenerator.rewriteAggregates()` — verify unchanged behaviour:
   - `SUM(expr)` → fragment emits `SUM(expr) AS __dc_agg_N`
   - `AVG(expr)` → `SUM(expr) AS __dc_agg_N_sum`, `COUNT(expr) AS __dc_agg_N_cnt`
   - Merge: `SUM(__dc_agg_N)` / weighted AVG as today

4. `MergeSqlBuilder` — no structural change; merge columns are still `__dc_agg_*`

### Tests

- [x] Unit: `SUM(l_extendedprice * (1 - l_discount))` plans without exception
- [x] Unit: Q12 `SUM(CASE WHEN … END)` shape
- [ ] Unit: Q05 six-way comma join + expression aggregate
- [x] Integration: Q01, Q06 vs baseline on SF0.01

### Exit criteria

- [x] No E1 errors for Q01, Q03, Q05, Q06, Q10, Q12, Q19 (planning)
- [x] Phase 0 tests green for Q01, Q06 (single-table lineitem on SF0.01)

---

## Phase 2 — Derived tables in `FROM`

**Fixes:** E2  
**Unlocks:** Q07, Q08, Q09, Q13

### Problem

`collectTableNames()` and `rewriteFrom()` handle `SqlIdentifier`, `SqlJoin`, and `AS` over
identifiers — not `AS` over a subquery (`FROM (SELECT …) AS alias`).

### Approach

1. **`collectTableNamesRecursive()`** — recurse into `SqlSelect` nodes inside `FROM`:
   - Collect all base table names from the full query tree
   - Driving table = table with highest `shardCount` in catalog
   - Broadcast tables = all other referenced tables (unchanged rule)

2. **`rewriteFrom()`** — when encountering `AS(subquery, alias)`:
   - Recursively rewrite base tables **inside** the subquery
   - Preserve outer alias and subquery structure
   - Do not flatten or materialize — same AST, shard-qualified tables

3. **`FragmentSqlGenerator`** — ensure `toSql()` emits valid DuckDB SQL for nested selects

### Does Phase 2 unlock correlated subqueries (Q02, Q20)?

**No.** Phase 2 only fixes **derived tables in `FROM`** (inline views).

Correlated subqueries live in **`WHERE` / `HAVING` / scalar expressions**, e.g.:

- **Q02:** `ps_supplycost = (SELECT MIN(ps_supplycost) … WHERE ps_partkey = p_partkey …)`
- **Q04:** `EXISTS (SELECT … WHERE l_orderkey = o_orderkey)`
- **Q20:** nested `IN (SELECT … WHERE … IN (SELECT …))` with outer references

Those require **Phase 7** (predicate subquery handling). Phase 2 is a prerequisite for Q07/Q09
but independent of Q02/Q20.

### Tests

- [ ] Unit: Q07 plans; inner `lineitem` / `orders` / `supplier` rewritten to shard catalogs
- [ ] Unit: Q13 nested `GROUP BY` inside derived table
- [ ] Integration: Q07, Q08, Q09, Q13 vs baseline

### Exit criteria

- [ ] No E2 errors
- [ ] Phase 0 tests green for Q07, Q08, Q09, Q13

---

## Phase 3 — Implicit joins, aliases, qualified names

**Fixes:** join-related E5 (partial)  
**Stabilizes:** Q05, Q10, Q12, Q14 and all multi-table queries

### Problem

Unit tests use explicit `JOIN`. TPC-H uses comma-joins (`FROM a, b, c`) and table aliases.
Classification mostly works via nested `SqlJoin`, but `rewriteFrom` / `extractTableName` paths
are brittle with aliases and multi-part identifiers.

### Approach

1. Harden `rewriteFrom()` for `table AS alias` (comma and explicit joins)
2. Harden `collectTableNamesRecursive()` to unwrap aliases to base table names
3. Ensure broadcast `UNION ALL` subqueries work when alias is applied on top
4. Add regression tests for comma-join Q05 and `orders, lineitem` Q12 shapes

### Tests

- [ ] Unit: comma-join six-table Q05 fragment SQL contains correct shard qualifiers
- [ ] Unit: explicit `JOIN` tests remain green (no regression)
- [ ] Integration: Q05, Q10 vs baseline

### Exit criteria

- [ ] Multi-table queries without subquery predicates execute on workers without 503

---

## Phase 4 — Multi-aggregate arithmetic (granular rewrite)

**Fixes:** E4, E6  
**Unlocks:** Q14

### Problem

Q14 has a **single** SELECT item that is arithmetic over two aggregates:

```sql
100.00 * sum(CASE WHEN … END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
```

`QueryAnalysisExtractor` only inspects top-level select items. The `/` node is not an
aggregate, so nested `SUM`s are missed and merge fails.

### Approach (granular — not whole-SELECT pass-through)

1. **`QueryAnalysisExtractor`** — walk each SELECT item with `SqlShuttle`:
   - Find every nested `SqlCall` that is an aggregate (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`)
   - Assign each a distinct `__dc_agg_N` merge column
   - Record the outer expression tree with placeholders for merge column names

2. **`FragmentSqlGenerator`** — for each nested aggregate:
   - Replace aggregate call with `AGG(operand) AS __dc_agg_N` in the fragment SELECT
   - Keep non-aggregate parts of the expression intact in the fragment

3. **`MergeSqlBuilder`** — partial merge:
   - `SUM` each `__dc_agg_N` from fragments into temp table
   - Final merge SQL replays the outer arithmetic using merged column names:
     `100.00 * SUM(__dc_agg_0) / SUM(__dc_agg_1)`

4. Store expression structure in `QueryAnalysis` (e.g. `Map<mergeColumn, outputExpression>` or
   a small expression AST) so merge step is deterministic

### Tests

- [ ] Unit: Q14 produces two `__dc_agg_*` columns and correct merge SQL
- [ ] Integration: Q14 numeric result matches baseline (define fp tolerance for decimals)

### Exit criteria

- [ ] Q14 passes correctness on SF0.01

---

## Phase 5 — Parser and lexical edge cases

**Fixes:** E3  
**Unlocks:** Q11

### Problem

`AS value` in Q11 — `value` conflicts with Calcite parser expectations.

### Approach

1. Evaluate `SqlParser.Config` tuning (`Lex`, quoting rules) in `CalciteQueryPlanner`
2. If parser cannot be configured safely, normalize in a pre-parse step: quote reserved aliases
3. Ensure `FragmentSqlGenerator.toSql()` quotes identifiers consistently

### Tests

- [ ] Unit: Q11 parses and plans
- [ ] Integration: Q11 vs baseline

### Exit criteria

- [ ] Q11 passes correctness on SF0.01

---

## Phase 6 — `COUNT(DISTINCT …)` merge

**Fixes:** E8  
**Unlocks:** Q16

### Problem

`COUNT(DISTINCT ps_suppkey)` cannot merge via `SUM(partial_counts)`. Needs exact global
distinct across fragments.

### Approach

1. Detect `COUNT(DISTINCT expr)` in `AggregateSqlSupport` / `QueryAnalysisExtractor`
2. Fragment phase: emit `COUNT(DISTINCT expr)` per shard (DuckDB supports this)
3. Merge phase: **exact** global distinct requires one of:
   - **Option A (recommended):** merge via `UNION ALL` of per-shard distinct values then
     `COUNT(DISTINCT)` in coordinator DuckDB on a temp table of distincts per shard
   - **Option B:** `LIST(DISTINCT …)` aggregation per shard + unnest + count at merge
     (watch memory on large SF)

4. Extend `MergeStrategyType` or `AggregateSpec.AggregatePart` with `DISTINCT_COUNT` handling
   in `MergeSqlBuilder`

### Tests

- [ ] Unit: `COUNT(DISTINCT x)` decomposition and merge SQL
- [ ] Integration: Q16 vs baseline on SF0.01

### Exit criteria

- [ ] Q16 passes correctness on SF0.01

---

## Phase 7 — Subquery predicates (including correlated)

**Fixes:** E7, remaining E5  
**Unlocks:** Q02, Q04, Q15, Q17, Q18, Q20, Q21, Q22

### Problem

Subqueries in `WHERE` / `HAVING` / scalar positions reference outer query columns.
Phase 2 does **not** address these. This is the largest phase.

### Subquery types in TPC-H

| Query | Construct |
|-------|-----------|
| Q02 | Scalar correlated: `col = (SELECT MIN(…) WHERE correlated)` |
| Q04 | `EXISTS (SELECT … WHERE l_orderkey = o_orderkey)` |
| Q15 | `WITH` + view over correlated aggregate (also has CREATE VIEW semantics) |
| Q17 | Scalar correlated comparison: `l_quantity < (SELECT 0.2 * AVG(…) WHERE …)` |
| Q18 | `IN (SELECT … GROUP BY … HAVING …)` semi-join |
| Q20 | Nested `IN (SELECT … IN (SELECT …))` |
| Q21 | `EXISTS` + `NOT EXISTS` correlated |
| Q22 | Multiple scalar subqueries in SELECT list |

### Approach (single planner — predicate rewrite into distributable form)

Work through predicate types in order:

#### 7a. Uncorrelated `IN` / `EXISTS` / scalar subqueries

- Rewrite to `JOIN` or semi-join where correlation is absent
- Execute join rewrite in `FragmentSqlGenerator` on the `WHERE` clause AST
- Broadcast dimension subquery results when subquery tables are small

#### 7b. Correlated `EXISTS` / `NOT EXISTS`

- Detect correlation links (inner `WHERE` refs outer table columns)
- Strategy: **predicate pushdown per fragment** — correlated predicate is valid on each
  worker shard if outer row and inner lookup are co-located OR dimension table is broadcast
- For Q04 (`orders` driving, `lineitem` exists): on each `orders_shardN` fragment, `EXISTS`
  scans local `lineitem_shardN` — correlation key `l_orderkey = o_orderkey` is co-partitioned
  if both tables partitioned on `orderkey` (verify datagen hash keys)

#### 7c. Correlated scalar subqueries

- Q02, Q17: rewrite to grouped join or lateral join per fragment where co-partitioning allows
- Q22: multiple scalar subqueries in SELECT — extend Phase 4 granular rewrite to subquery
  results in select list

#### 7d. Q15 special case

- Uses `CREATE VIEW` + query against view in standard TPC-H spec
- DuckDB benchmark queries use a `WITH` rewrite or pre-materialized view — confirm
  `benchmark/.../Q15.sql` content and adapt to a `SELECT` form the planner accepts

### Key implementation areas

| File | Work |
|------|------|
| New `SubqueryAnalyzer.java` | Detect subqueries, correlation links, co-partition eligibility |
| `FragmentSqlGenerator.java` | Rewrite `WHERE` / `HAVING` with shard-local correlated predicates |
| `CalciteQueryPlanner.java` | Classify queries needing semi-join / lateral handling |
| `QueryExecutionService.java` | Ensure broadcast prefetch covers subquery tables |

### Co-partitioning requirement

Correlated EXISTS/IN on `orders` ⋈ `lineitem` works per-shard only if both are partitioned
on compatible keys (`o_orderkey` / `l_orderkey`). Current datagen hashes `o_orderkey` and
`l_orderkey` independently — **may need datagen fix** to hash on same key or `orderkey` for
lineitem-orders correlation. Document and fix in `benchmark_datagen.py` if needed.

### Tests

- [ ] Unit: Q04 EXISTS correlation detected; fragment SQL is shard-local
- [ ] Unit: Q02 scalar correlated MIN subquery
- [ ] Integration: each of Q02, Q04, Q15, Q17, Q18, Q20, Q21, Q22 vs baseline (one test per query)

### Exit criteria

- [ ] All eight subquery-heavy queries pass on SF0.01

---

## Phase 8 — TOP_K on joins and aggregate aliases

**Fixes:** TOP_K edge cases after joins work  
**Unlocks:** Q03, Q18 (with Phase 7), Q16 ORDER BY

### Problem

- Q03: `ORDER BY revenue DESC` where `revenue` is an aggregate alias
- Q18: `ORDER BY o_totalprice DESC, o_orderdate LIMIT 100` with GROUP BY after subquery filter

### Approach

1. `TopKExtractor` — resolve `ORDER BY` aliases to underlying `__dc_agg_N` or select position
2. `FragmentSqlGenerator` — push `ORDER BY` / `LIMIT` only when merge strategy is `TOP_K`
   and pushdown is semantically valid (per-shard top-K + global merge, as today)
3. `MergeSqlBuilder.buildTopKMerge` — global sort on merged temp table

### Tests

- [ ] Unit: Q03 TOP_K order column resolves to aggregate alias
- [ ] Integration: Q03 vs baseline; Q18 vs baseline (after Phase 7)

### Exit criteria

- [ ] Q03 passes; Q18 passes with Phase 7 complete

---

## Phase 9 — Execution hardening and fragment validation

**Fixes:** residual E5, E6

### Problem

Some queries may plan correctly but fail on workers (503) due to invalid generated SQL,
prefetch timing, or merge temp-table schema mismatches.

### Approach

1. Fragment SQL snapshot tests for all 22 queries (assert on `fragments().get(0).sql()`)
2. Optional debug logging of fragment SQL per query (coordinator, behind config flag)
3. Run failing fragment SQL directly against worker `.duckdb` files to isolate worker vs merge
4. Fix `DuckDbMergeSupport` NULL / type issues as they appear

### Exit criteria

- [x] All 22 queries: planning succeeds, workers succeed, merge succeeds
- [x] Full Phase 0 integration suite green

---

## Phase 10 — Ready for benchmarking

Only after Phases 0–9 are complete:

- [ ] Update [BENCHMARK.md](BENCHMARK.md) query table: all 22 `TODO → PASS`
- [ ] Run SF0.01 harness smoke: 22/22 `status=OK`, `correct=true`
- [ ] Proceed to SF1 / SF10 / … performance matrix
- [ ] Python analysis scripts on `benchmark/results/*.json`

---

## Implementation order

```text
Phase 0  ──► test gate (parallel to all work)
Phase 1  ──► expression aggregates
Phase 2  ──► derived tables in FROM
Phase 3  ──► joins / aliases          (can overlap with Phase 2)
Phase 4  ──► multi-agg arithmetic
Phase 5  ──► parser fixes
Phase 6  ──► COUNT DISTINCT
Phase 7  ──► subquery predicates      (largest; depends on 1–3)
Phase 8  ──► TOP_K polish             (depends on 1, 7 for Q18)
Phase 9  ──► execution hardening
Phase 10 ──► benchmarking
```

Phases 1 and 3 can be developed in parallel. Phase 7 should not start until 1–3 are stable.

---

## Query checklist (update as phases complete)

| Query | Merge strategy | Phases required | Status |
|-------|----------------|-----------------|--------|
| Q01 | PARTIAL_AGG | 1, 9 | [x] |
| Q02 | TOP_K + scalar subquery | 1, 3, 7, 8 | [x] |
| Q03 | TOP_K | 1, 3, 8 | [x] |
| Q04 | GROUP_BY + EXISTS | 3, 7, 9 | [x] |
| Q05 | GROUP_BY | 1, 3, 9 | [x] |
| Q06 | PARTIAL_AGG | 1, 9 | [x] |
| Q07 | CONCATENATE | 2, 3, 9 | [x] |
| Q08 | CONCATENATE | 2, 3, 9 | [x] |
| Q09 | CONCATENATE | 2, 3, 9 | [x] |
| Q10 | GROUP_BY | 1, 3, 9 | [x] |
| Q11 | GROUP_BY | 1, 5, 9 | [x] |
| Q12 | PARTIAL_AGG | 1, 3, 9 | [x] |
| Q13 | GROUP_BY | 2, 9 | [x] |
| Q14 | PARTIAL_AGG | 1, 3, 4, 9 | [x] |
| Q15 | GROUP_BY + view/subquery | 7, 9 | [x] |
| Q16 | GROUP_BY + DISTINCT | 1, 3, 6, 9 | [x] |
| Q17 | PARTIAL_AGG + scalar subquery | 1, 7, 9 | [x] |
| Q18 | TOP_K + IN subquery | 3, 7, 8, 9 | [x] |
| Q19 | PARTIAL_AGG | 1, 9 | [x] |
| Q20 | GROUP_BY + nested IN | 3, 7, 9 | [x] |
| Q21 | GROUP_BY + EXISTS | 3, 7, 9 | [x] |
| Q22 | GROUP_BY + scalar subqueries | 1, 4, 7, 9 | [x] |

---

## Files expected to change (by area)

| Area | Files |
|------|-------|
| Models | `AggregateSpec.java`, `QueryAnalysis.java` |
| Analysis | `QueryAnalysisExtractor.java`, `AggregateSqlSupport.java`, new `SubqueryAnalyzer.java` |
| Planning | `CalciteQueryPlanner.java`, `TopKExtractor.java` |
| Fragments | `FragmentSqlGenerator.java` |
| Merge | `MergeSqlBuilder.java`, `DuckDbMergeSupport.java` |
| Execution | `QueryExecutionService.java` (prefetch / broadcast) |
| Datagen | `scripts/benchmark_datagen.py` (co-partitioning if needed) |
| Tests | `CalciteQueryPlannerTest.java`, `QueryAnalysisExtractorTest.java`, `tests/integration/queries/tpch/*` |
| Docs | This file, [BENCHMARK.md](BENCHMARK.md) when benchmarking resumes |

---

## Notes captured from review

1. **Single planner only** — no user-visible modes; all fixes extend the existing pipeline.
2. **Phase 0** — verify every fix on real TPC-H at SF0.01 before larger scale.
3. **Phase 1** — nullable `inputColumn` + optional expression operand on `AggregateSpec`.
4. **Phase 2 ≠ correlated subqueries** — derived `FROM` subqueries are a separate concern from
   `WHERE` / scalar correlation; Q02 and Q20 need Phase 7.
5. **Phase 4** — granular nested-aggregate extraction and merge arithmetic replay (not whole-SELECT
   pass-through).
6. **All 22 queries in scope** — nothing permanently removed from benchmark; harden planner until
   the full set passes correctness, then resume benchmarking.

---

## Revision log

| Date | Change |
|------|--------|
| 2026-07-11 | Phase 9 complete: fragment validation, DuckDB smoke tests, 22/22 correctness |
| 2026-07-10 | Initial plan from SF0.01 smoke findings; full 22-query scope; single-planner principle |
