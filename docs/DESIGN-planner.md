# Query Planner — Design Decisions

This document records **why** the DuckCluster planner works the way it does and **what was
implemented** to reach full TPC-H correctness on SF0.01. It is meant to be committed and updated
as planner behavior evolves.

For the implementation checklist and query-by-query tracking, see `PLAN-tpch-planner-hardening.md`
(local only). For system-wide architecture, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. What Calcite does here (and what it does not)

### What we use Calcite for

| Capability | How |
|------------|-----|
| SQL parsing | `SqlParser` with `Lex.JAVA`, `PRAGMATIC_2003` conformance |
| AST (`SqlNode`) | `SqlSelect`, `SqlJoin`, `SqlOrderBy`, etc. |
| SQL pretty-printing | `SqlPrettyWriter` + `DuckDBSqlDialect` in `FragmentSqlGenerator` |

Calcite is effectively a **parser + AST toolkit + SQL printer** in the current codebase.

### What we do *not* use Calcite for (yet)

| Capability | Status |
|------------|--------|
| `SqlValidator` / type checking | Not wired |
| `RelNode` (logical relational algebra) | Not built |
| Hep / Volcano optimizer rules | Not used |
| Distribution traits (partitioning, broadcast, exchange) | Not modeled |
| Cost-based join ordering | Not used |
| Automatic distributed plan generation | Not used |

`ARCHITECTURE.md` describes a future state ("parse → validate → relational plan → SQL generation").
The TPC-H hardening work implemented **parse → hand-rolled analysis → manual rewrite → merge**.

### Why this feels complex

Calcite *can* plan distributed queries, but only after you invest in the full stack:

1. Schema + statistics
2. SQL → `RelNode` conversion
3. Custom traits for shard distribution / broadcast
4. Rules that insert `Exchange` / `Union` / `Aggregate` with correct collation
5. An execution engine that understands those physical operators

DuckCluster skipped that stack and instead built a **custom shard-aware rewriter** on top of the
`SqlNode` AST. Every TPC-H gap we fixed (derived tables, nested `GROUP BY`, expression aggregates,
multi-aggregate arithmetic, correlated subqueries) is logic that a full Calcite integration would
eventually encode as optimizer rules — but we wrote it explicitly, query-shape by query-shape.

**Analogy:** Calcite is a compiler IR + optimization framework, not a distributed database.
We use it as a lexer/parser, then wrote our own middle-end.

---

## 2. Core planner pipeline (single code path)

```
SQL
 └─► Calcite parse (SqlNode)
      └─► classifyTables (driving + broadcast, from catalog shard counts)
           └─► detectMergeStrategy (CONCATENATE | PARTIAL_AGG | GROUP_BY_MERGE | NESTED_GROUP_BY_MERGE | TOP_K)
                └─► QueryAnalysisExtractor (group keys, aggregates, output columns)
                     └─► FragmentSqlGenerator (shard-qualified FROM, partial-agg rewrite)
                          └─► Workers execute fragments
                               └─► MergeSqlBuilder + coordinator DuckDB
```

There is no alternate "simple" planner path. All TPC-H fixes extend this pipeline.

---

## 3. TPC-H hardening — what was achieved

The planner was hardened incrementally until all **22 TPC-H queries** passed end-to-end on
SF0.01 (planning, worker fragment execution, coordinator merge). Milestones below are grouped
by capability, not by implementation order.

### Expression aggregates

**Unlocked:** Q01, Q06

Extended `AggregateSpec` with optional `inputExpression` so `SUM(l_extendedprice * (1 - l_discount))`,
`SUM(CASE WHEN …)`, and similar shapes analyze and rewrite without bare-column operands. Merge
still uses `SUM(__dc_agg_N)` over partials.

### Derived tables in `FROM`

**Unlocked:** Q07, Q09, Q13 (Q13 also required nested `GROUP BY` merge — see D7)

Recursed into `SqlSelect` nodes in table collection and `rewriteFrom`. Preserved subquery
structure; only qualified base tables inside inline views.

### Comma-joins, aliases, and tied driving-table selection

**Unlocked:** Q05, Q10, Q12

Added `TableNameSupport.baseTableName()`, fixed alias vs table confusion in
`collectTableNamesRecursive()`, and made tied shard counts deterministic (last table in FROM
traversal wins). `MergeSqlBuilder` emits columns in `outputColumnNames` order (Q10).

### Nested aggregate arithmetic

**Unlocked:** Q08, Q14

`NestedAggregateExtractor` walks expression trees, assigns `__dc_agg_N` per nested aggregate,
and `ComputedOutputSpec` replays arithmetic at merge time with `SUM(__dc_agg_N)`.

### Parser edge cases

**Unlocked:** Q11

`SqlNormalizer` quotes reserved aliases like `value`; parser uses `Quoting.DOUBLE_QUOTE`.
HAVING/WHERE scalar subqueries get shard-qualified `FROM` via `FragmentSqlGenerator.rewriteSubqueries()`.

### `COUNT(DISTINCT …)` global merge

**Unlocked:** Q16

Fragment SQL emits group keys plus the distinct operand column; merge runs
`COUNT(DISTINCT __dc_distinct_N)` on unioned shard rows. Subquery-only catalog tables are
shard-qualified via the full `ClusterCatalog` map.

### Uncorrelated predicate subqueries

**Unlocked:** Q18

`SubqueryAnalyzer` detects correlation via TPC-H column-prefix rules. Uncorrelated subqueries
rewrite inner `FROM` with `globalSubqueryScope` → `UNION ALL` across all catalog shards.
`PlannedQuery.subqueryBroadcastTables` drives coordinator prefetch before execution.

### Correlated `EXISTS` / `NOT EXISTS` with co-located shards

**Unlocked:** Q04, Q21

`FromScope` tracks aliases; correlated subqueries use `CO_LOCATED_SUBQUERY` rewrite — inner
tables scan `{table}_shard{N}` matching the fragment. `PlannedQuery.correlatedCoPartitionTables`
prefetches co-located inner shards before execution.

### Correlated scalar subqueries and derived-table predicates

**Unlocked:** Q02, Q17, Q20, Q22

Restricted co-location to correlated `EXISTS` only; scalar and comparison subqueries use
`GLOBAL_SUBQUERY` (`UNION ALL`). Added `canCoLocateSubquery`, `rewriteEmbeddedSelect` for
derived-table `WHERE`/`HAVING`, accumulated `FromScope` for nested `IN`, and fixed TOP_K merge
to use native column types (no blanket `CAST(… AS DOUBLE)`).

### `WITH` / CTE coordinator two-step merge

**Unlocked:** Q15

Detected `SqlWith`; planned CTE body as shard-local `GROUP BY` on the CTE driving table.
Coordinator runs inner `GROUP_BY_MERGE` → `__merge_temp`, then `WithCteOuterSqlGenerator` +
`WithCteMergeStrategy` for the outer query.

### TOP_K and `ORDER BY` on aggregate aliases

**Unlocked:** Q03, Q19 (Q16 `ORDER BY` polish)

`TopKExtractor` and `TopKResolver` resolve `ORDER BY` against `SELECT` aliases and
`QueryAnalysis` metadata. `MergeSqlBuilder` emits `ORDER BY SUM("__dc_agg_N")` for aggregate
order keys.

### Fragment validation and execution hardening

**Unlocked:** full 22/22 gate

`FragmentSqlValidator` enforces structural fragment SQL rules. `TpchFragmentSnapshotTest` and
`TpchFragmentDuckDbTest` validate all queries at plan time and against worker shard files.
`DUCKCLUSTER_LOG_FRAGMENT_SQL` logs fragment SQL before dispatch. `DuckDbMergeSupport.normalizeValue`
maps empty strings to `NULL` for numeric merge columns.

---

## 4. Decision log

Decisions are listed in roughly chronological / dependency order. Each entry states the problem,
the decision, and the trade-off.

### D1 — AST rewriter instead of RelNode optimizer

**Problem:** Need shard-local SQL strings for N workers plus a coordinator merge step.

**Decision:** Stay on `SqlNode`; manually rewrite `FROM`, aggregates, and merge metadata.

**Trade-off:** Faster to ship for demo queries (`events` table). Does not scale to full SQL
generically; each new grammar shape needs explicit code.

**Alternative considered:** Calcite `RelNode` + custom `RelDistribution` traits. Rejected for
initial TPC-H scope; may revisit now that 22/22 pass.

---

### D2 — Driving table = highest shard count; everything else broadcast

**Problem:** Tables are sharded independently (different keys, counts, ring placement).

**Decision:** Pick the table with the most shards as the **driving table** (one fragment per
shard). All other referenced tables are **broadcast** via `UNION ALL` subqueries in fragment SQL.
Missing broadcast shards are prefetched to workers before execution.

**Trade-off:** Simple and correct for many star/snowflake joins. Moves more data when both sides
are large. Documented in `DESIGN-DECISIONS.md` §7.

**Why Calcite doesn't solve this:** Join distribution is a physical planning decision. Calcite
would need cluster metadata (shard counts, colocation) encoded in traits. We encode it in
`classifyTables()` + `ClusterCatalog`.

---

### D3 — Explicit merge strategies (not SQL pass-through)

**Problem:** Partial aggregates on shards are not final answers (`SUM` of `SUM`, `COUNT(*)` over
histogram buckets, etc.).

**Decision:** Classify each query into a `MergeStrategyType` and run coordinator-side DuckDB SQL
from `MergeSqlBuilder`.

| Strategy | Worker emits | Coordinator merge |
|----------|--------------|-------------------|
| `CONCATENATE` | Raw rows | Append |
| `PARTIAL_AGG` | One partial agg row per shard | `SUM`/`MIN`/`MAX` over merge columns |
| `GROUP_BY_MERGE` | Partial groups | Re-`GROUP BY` + merge aggs |
| `NESTED_GROUP_BY_MERGE` | Inner partial groups only | Two-step: merge inner, then outer `GROUP BY` on coordinator |
| `WITH_CTE_MERGE` | CTE inner partial groups | Two-step: merge CTE body, then outer SQL on coordinator |
| `TOP_K` | Locally sorted/limited rows | Global `ORDER BY` + `LIMIT` |

**Trade-off:** Merge logic must mirror fragment rewrite (`__dc_agg_N` column names). Correctness
is test-driven per query shape.

---

### D4 — Expression aggregates: nullable `inputColumn` + `inputExpression`

**Problem:** TPC-H uses `SUM(l_extendedprice * (1 - l_discount))`, `SUM(CASE WHEN …)`, etc.
`QueryAnalysisExtractor` originally required bare column operands.

**Decision:** Extended `AggregateSpec` with optional `inputExpression`. Analysis never throws on
expressions; fragment SQL preserves the full operand `SqlNode`; merge still uses
`SUM(__dc_agg_N)` over partials.

**Trade-off:** Merge metadata tracks merge column names, not expression trees. Sufficient for
additive aggregates (`SUM`, `COUNT`). Non-decomposable aggregates (`COUNT(DISTINCT)`, median)
needed separate strategies (D12).

**Unlocked:** Q01, Q06

---

### D5 — Derived tables in `FROM`: recurse, do not flatten

**Problem:** `FROM (SELECT …) AS alias` made `collectTableNames()` return empty and `rewriteFrom()`
skip shard qualification inside the subquery.

**Decision:** Recursed into `SqlSelect` nodes in both collection and rewrite. Preserved subquery
structure; only qualified base tables inside. Supported `AS(subquery, alias, col, …)` via
`recreateAsCall()`.

**Trade-off:** Correct for Q07/Q09-style inline views (inner query is relational, outer query
aggregates). Insufficient when inner query has its own `GROUP BY` over cross-shard data (Q13 —
addressed in D7).

**Why Calcite doesn't solve this alone:** Parsing nested `FROM` is easy. Knowing *where* the
subquery may execute relative to shards is a distribution decision — still ours to make.

**Unlocked:** Q07, Q09

---

### D6 — ORDER BY on `GROUP BY` queries: defer to coordinator merge

**Problem:** Fragment SQL rewrote `COUNT(*) AS custdist` to `COUNT(*) AS __dc_agg_0`. Appending
outer `ORDER BY custdist` to fragment SQL failed (alias not found). Even when the alias exists,
per-shard ordering is meaningless before global merge.

**Decision:** If `analysis.groupByColumns()` is non-empty, do **not** push `ORDER BY`/`LIMIT` to
fragments. Apply them in `MergeSqlBuilder.buildGroupByMerge(..., topK)` on the coordinator.

**Trade-off:** Slightly more coordinator work; semantically correct. TOP_K on aggregate aliases
(D17) built on this foundation for Q03 and Q19.

**Unlocked:** Q13 coordinator ordering; foundation for D17

---

### D7 — Nested `GROUP BY` in derived tables: `NESTED_GROUP_BY_MERGE`

**Problem:** Q13 structure:

```sql
SELECT c_count, COUNT(*) AS custdist
FROM (
  SELECT c_custkey, COUNT(o_orderkey)
  FROM customer LEFT JOIN orders …
  GROUP BY c_custkey
) AS c_orders (c_custkey, c_count)
GROUP BY c_count
```

Running the full query on each shard computes a **local histogram of local per-customer counts**.
Merging those histograms with `GROUP_BY_MERGE` is wrong — a customer's orders span shards.

**Decision:** Detected `AS(inner Select with GROUP BY, …)` via `NestedDerivedTableDetector`.

1. **Fragments:** execute **inner** `SELECT` only (shard-qualified).
2. **Coordinator step 1:** `GROUP BY` merge on inner keys → one row per customer with global
   order count (`buildGroupByMerge` with derived column aliases).
3. **Coordinator step 2:** run **outer** aggregation as local SQL on step-1 rows
   (`CoordinatorOuterSqlGenerator` — uses `COUNT(*)`, not `SUM(__dc_agg_N)`).

**Trade-off:** Extra merge strategy and coordinator temp-table round trip. Only triggered for
derived-table + inner-`GROUP BY` pattern. A full optimizer would plan this as
`Exchange` → partial agg → `Exchange` → final agg.

**Unlocked:** Q13

---

### D8 — Multi-aggregate arithmetic in one SELECT item

**Problem:** `SUM(CASE …) / SUM(volume)` is one SELECT item with arithmetic over two aggregates.

**Decision:** `NestedAggregateExtractor` walks expression trees, assigns `__dc_agg_N` per nested
aggregate, rewrites fragments to emit partials only, and stores `ComputedOutputSpec` merge
expressions that replay arithmetic with `SUM(__dc_agg_N)`.

**Unlocked:** Q08, Q14

---

### D9 — Parser edge cases handled in planner, not by "more Calcite"

**Examples:** trailing semicolons (strip in `parse()`), reserved aliases like `value` (D11).

**Decision:** Prefer small normalizations at the DuckCluster boundary over forking Calcite.

**Trade-off:** Masks client SQL quirks; keeps worker/merge SQL clean.

---

### D10 — Comma-joins, aliases, and tied driving-table selection

**Problem:** TPC-H uses `FROM a, b, c` comma-joins and `table AS alias`. Table classification and
`rewriteFrom` needed consistent base-table resolution. When two tables share the same shard count
(e.g. `orders` and `lineitem` both have 6), driving-table choice was non-deterministic.

**Decision:**

1. `TableNameSupport.baseTableName()` unwraps `AS` aliases and `catalog.table` qualifiers.
2. `collectTableNamesRecursive()` uses `TableNameSupport` so aliases like `n1` are not mistaken
   for tables.
3. Tied shard counts: **last table in FROM traversal order** wins (`>=` comparison).
4. `rewriteFrom` preserves aliases on broadcast `UNION ALL` subqueries.
5. `MergeSqlBuilder.buildGroupByMerge` emits columns in **`outputColumnNames` order** (fixes
   Q10 column ordering vs baseline).

**Trade-off:** Tied driving-table choice is heuristic but correct either way when shard counts
match.

**Unlocked:** Q05, Q10, Q12

---

### D11 — Reserved alias `value`

**Problem:** Calcite parser rejects `AS value` (reserved word).

**Decision:** `SqlNormalizer` quotes `AS value` and `ORDER BY value`; parser uses
`Quoting.DOUBLE_QUOTE`. HAVING/WHERE scalar subqueries get shard-qualified `FROM` via
`FragmentSqlGenerator.rewriteSubqueries()`.

**Unlocked:** Q11

---

### D12 — `COUNT(DISTINCT …)` merge via per-shard distinct values

**Problem:** `COUNT(DISTINCT x)` partial counts cannot be summed across shards.

**Decision:** Fragment SQL emits group keys plus the distinct operand column (expanded `GROUP BY`);
merge SQL runs `COUNT(DISTINCT __dc_distinct_N)` on a temp table of unioned shard rows.
`AggregateSpec.AggregatePart.DISTINCT_COUNT` drives fragment rewrite and `MergeSqlBuilder` merge
expression. Subquery-only catalog tables (e.g. `supplier` in Q16 `NOT IN`) are shard-qualified
via the full `ClusterCatalog` map, not only main-FROM broadcast tables.

**Unlocked:** Q16

---

### D13 — Uncorrelated predicate subqueries with global shard union

**Problem:** `IN (SELECT … FROM lineitem …)` subqueries without outer references must see all
shards of inner tables. Per-fragment single-shard rewrite is incomplete; `UNION ALL` over missing
remote shards fails on workers.

**Decision:**

1. `SubqueryAnalyzer` detects correlation via TPC-H column-prefix rules (`p_`, `l_`, `o_`, …).
2. Uncorrelated subqueries rewrite inner `FROM` with `globalSubqueryScope` → `UNION ALL` across
   all catalog shards of multi-shard tables.
3. `PlannedQuery.subqueryBroadcastTables` drives coordinator prefetch of those shards to fragment
   workers before execution (same path as join broadcast prefetch).

**Unlocked:** Q18

---

### D14 — Correlated `EXISTS`/`NOT EXISTS` with co-located shards

**Problem:** Correlated `EXISTS`/`NOT EXISTS` must join outer rows to inner lookup rows on the
same partition (e.g. `orders` ⋈ `lineitem` on `orderkey`). Alias-qualified outer refs (`l1` in
`l2.l_orderkey = l1.l_orderkey`) were misclassified as uncorrelated.

**Decision:**

1. `FromScope` tracks table aliases; correlation detects outer alias references.
2. Correlated subqueries use `CO_LOCATED_SUBQUERY` rewrite — inner tables scan `{table}_shard{N}`
   matching the fragment, not broadcast `UNION ALL`.
3. `PlannedQuery.correlatedCoPartitionTables` prefetches co-located inner shards (e.g. `lineitem`
   for `orders` EXISTS) before fragment execution.

**Unlocked:** Q04, Q21

---

### D15 — Correlated scalar subqueries and derived-table predicates

**Problem:** Scalar comparisons (`col > (SELECT AVG …))`) and nested `IN` predicates need global
`UNION ALL` when the inner table is sharded but the outer row is not co-partitioned (e.g. Q20
`lineitem` correlated to single-shard `partsupp`). Derived tables in `FROM` (Q22) left subqueries
in their `WHERE` unqualified. TOP_K merge cast all `ORDER BY` columns to `DOUBLE`, breaking
string sorts (Q02).

**Decision:**

1. **EXISTS-only co-location** — `CO_LOCATED_SUBQUERY` applies only to correlated `EXISTS`;
   scalar and comparison subqueries use `GLOBAL_SUBQUERY` (`UNION ALL`).
2. **`canCoLocateSubquery`** — skip co-location when the driving table has one shard and the inner
   table is multi-shard (Q22 `NOT EXISTS` on `orders` with single-shard `customer`).
3. **Embedded SELECT rewrite** — `rewriteEmbeddedSelect` rewrites `WHERE`/`HAVING` inside derived
   tables; `FromScope` recurses into subquery `FROM` clauses.
4. **Accumulated scope** — nested subqueries extend `FromScope` so correlation is detected
   against the correct outer tables (Q20 nested `IN`).
5. **TOP_K merge** — `ORDER BY` uses native column types (no blanket `CAST(… AS DOUBLE)`).

**Unlocked:** Q02, Q17, Q20, Q22

---

### D16 — `WITH`/CTE coordinator two-step merge

**Problem:** Q15 uses `WITH revenue AS (SELECT … FROM lineitem GROUP BY …)`. Calcite parses this
as `SqlWith`; the planner only accepted bare `SELECT`.

**Decision:**

1. Detected `SqlWith` (including `ORDER BY` wrapper) and planned the CTE body as shard-local
   `GROUP BY` fragments on the CTE driving table (`lineitem`).
2. Coordinator step 1: `GROUP_BY_MERGE` → `__merge_temp`.
3. Coordinator step 2: `WithCteOuterSqlGenerator` rewrites the outer query to join `__merge_temp`
   in place of the CTE name; `WithCteMergeStrategy` runs outer SQL in coordinator DuckDB.
4. Coordinator dimension tables (e.g. `supplier`) are fetched from a worker before step 2.

**Unlocked:** Q15

---

### D17 — TOP_K and `ORDER BY` on aggregate aliases

**Problem:** `GROUP BY` queries with `ORDER BY` on aggregate aliases (`revenue`, `supplier_cnt`,
`value`) need merge SQL that sorts on the merged aggregate expression, not a bare alias that may
not exist in `__merge_temp`.

**Decision:**

1. `TopKExtractor` resolves `ORDER BY` names against `SELECT` list aliases.
2. `TopKResolver` validates order columns against `QueryAnalysis` output metadata.
3. `MergeSqlBuilder` emits `ORDER BY SUM("__dc_agg_N")` (or `COUNT(DISTINCT …)`) for aggregate
   order keys; group-by columns still sort by quoted name.
4. `ORDER BY` / `LIMIT` remain on the coordinator for `GROUP_BY_MERGE`; fragments omit them.

**Unlocked:** Q03, Q19; Q16 `ORDER BY` polish

---

### D18 — Fragment validation and execution hardening

**Problem:** Queries could plan correctly but fail on workers (invalid SQL, prefetch timing) or
during coordinator merge (NULL/type mismatches in temp-table inserts).

**Decision:**

1. `FragmentSqlValidator` enforces structural fragment SQL rules: `SELECT` prefix, no trailing
   `;`, no bare multi-shard `FROM "table"`, fragment count matches driving shard count, driving
   `_shardN` reference for multi-shard tables.
2. `TpchFragmentSnapshotTest` validates all 22 queries: fragments pass validator + merge SQL
   smoke build.
3. `TpchFragmentDuckDbTest` runs first fragment against SF0.01 worker shard `.duckdb` files
   (`SELECT * FROM (fragment) LIMIT 1`) when benchmark data is present.
4. `DUCKCLUSTER_LOG_FRAGMENT_SQL` / `ClusterConfig.logFragmentSql()` logs each fragment SQL at
   INFO in `QueryExecutionService` before dispatch.
5. `DuckDbMergeSupport.normalizeValue` maps empty strings to `NULL` for numeric merge columns.

**Unlocked:** full 22/22 TPC-H correctness gate on SF0.01

---

## 5. Query status (planner + execution)

All 22 TPC-H queries pass end-to-end on SF0.01 as of July 2026.

| Query | End-to-end | Primary fix | Notes |
|-------|------------|-------------|-------|
| Q01 | ✅ | D4 | Expression `SUM` on `lineitem` |
| Q02 | ✅ | D15 | Correlated scalar + TOP_K string `ORDER BY` |
| Q03 | ✅ | D17 | TOP_K on aggregate alias `revenue` |
| Q04 | ✅ | D14 | Correlated `EXISTS` on `orders`/`lineitem` |
| Q05 | ✅ | D10 | Comma-join six tables |
| Q06 | ✅ | D4 | Expression `SUM(CASE …)` |
| Q07 | ✅ | D5 | Derived table in `FROM` |
| Q08 | ✅ | D8 | Multi-aggregate arithmetic |
| Q09 | ✅ | D5 | Derived table in `FROM` |
| Q10 | ✅ | D10 | Comma-join + alias + column order |
| Q11 | ✅ | D11 | Reserved alias `value` + HAVING subquery rewrite |
| Q12 | ✅ | D10 | `orders, lineitem` comma-join |
| Q13 | ✅ | D5 + D7 | Nested inner `GROUP BY` |
| Q14 | ✅ | D8 | Nested aggregates in expression |
| Q15 | ✅ | D16 | `WITH` CTE coordinator two-step merge |
| Q16 | ✅ | D12 | `COUNT(DISTINCT …)` |
| Q17 | ✅ | D15 | Correlated scalar on `lineitem` |
| Q18 | ✅ | D13 | Uncorrelated `IN` + global `lineitem` subquery |
| Q19 | ✅ | D4 + D17 | Partial agg expression on join |
| Q20 | ✅ | D15 | Nested `IN` + global correlated scalar |
| Q21 | ✅ | D14 | Correlated `EXISTS` + `NOT EXISTS` |
| Q22 | ✅ | D15 | Derived table + scalar + `NOT EXISTS` |

---

## 6. What "Calcite handles it" would actually require

If the goal is for Calcite to own distributed planning end-to-end, the project would need roughly:

1. **`DuckClusterSchema`** — expose each `(table, shardId)` as a partition or custom table scan
2. **SQL → `RelNode`** — `PlannerImpl` with cluster metadata (shard counts, broadcast costs)
3. **Traits** — e.g. `SINGLETON`, `HASH(c_custkey)`, `ROUND_ROBIN`
4. **Rules** — broadcast join, aggregate splitting, exchange insertion
5. **Physical SQL** — `RelToSqlConverter` per fragment with partition pruning
6. **Merge as final `Aggregate`/`Sort`** — or a coordinator `EnumerableRel` implementation

That is a multi-month optimizer project. The current approach traded generality for a **working
TPC-H correctness gate** on a fixed execution model (broadcast + partial agg + DuckDB merge).

**Open question (not decided):** Now that 22/22 TPC-H pass, do we incrementally move shapes from
hand-rolled `SqlNode` rewrite into Calcite rules, or keep the explicit pipeline and widen coverage?

---

## 7. Key source files

| Area | Files |
|------|-------|
| Entry point | `common/.../CalciteQueryPlanner.java` |
| Table classification | `classifyTables`, `collectTableNamesRecursive` |
| Nested derived tables | `NestedDerivedTableDetector.java`, `NestedDerivedTableSpec.java` |
| Analysis | `QueryAnalysisExtractor.java`, `AggregateSpec.java` |
| Fragment SQL | `FragmentSqlGenerator.java` |
| Merge SQL | `MergeSqlBuilder.java` |
| Nested merge execution | `NestedGroupByMergeStrategy.java`, `CoordinatorOuterSqlGenerator.java`, `DuckDbMergeSupport.java` |
| Integration gate | `tests/integration/test_tpch.py`, `tests/integration/duckcluster/tpch.py` |
| Fragment validation | `FragmentSqlValidator.java`, `TpchFragmentSnapshotTest.java`, `TpchFragmentDuckDbTest.java` |

---
