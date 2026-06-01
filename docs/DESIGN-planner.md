# Query Planner — Design Decisions

This document records **why** the DuckCluster planner works the way it does. It is meant to be
committed and updated as planner behavior evolves.

For the phase-by-phase implementation checklist, see `PLAN-tpch-planner-hardening.md` (local only).
For system-wide architecture, see [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. What Calcite does here (and what it does not)

### What we use Calcite for today

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
Phase 0–2 implementation stops at **parse → hand-rolled analysis → manual rewrite → merge**.

### Why this feels complex

Calcite *can* plan distributed queries, but only after you invest in the full stack:

1. Schema + statistics
2. SQL → `RelNode` conversion
3. Custom traits for shard distribution / broadcast
4. Rules that insert `Exchange` / `Union` / `Aggregate` with correct collation
5. An execution engine that understands those physical operators

DuckCluster skipped that stack and instead built a **custom shard-aware rewriter** on top of the
`SqlNode` AST. Every TPC-H gap we fix (derived tables, nested `GROUP BY`, expression aggregates,
multi-aggregate arithmetic, correlated subqueries) is logic that a full Calcite integration would
eventually encode as optimizer rules — but we are writing it explicitly, query-shape by
query-shape.

**Analogy:** Calcite is an compiler IR + optimization framework, not a distributed database.
We are using it as a lexer/parser, then writing our own middle-end.

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

There is no alternate "simple" planner path. Gaps are fixed in this pipeline.

---

## 3. Decision log

Decisions are listed in roughly chronological / dependency order. Each entry states the problem,
the decision, and the trade-off.

### D1 — AST rewriter instead of RelNode optimizer

**Problem:** Need shard-local SQL strings for N workers plus a coordinator merge step.

**Decision:** Stay on `SqlNode`; manually rewrite `FROM`, aggregates, and merge metadata.

**Trade-off:** Faster to ship for demo queries (`events` table). Does not scale to full SQL
generically; each new grammar shape needs explicit code.

**Alternative considered:** Calcite `RelNode` + custom `RelDistribution` traits. Rejected for
Phase 0 scope; may revisit after TPC-H correctness gate.

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
| `NESTED_GROUP_BY_MERGE` | Inner partial groups only | Two-phase: merge inner, then outer `GROUP BY` on coordinator |
| `TOP_K` | Locally sorted/limited rows | Global `ORDER BY` + `LIMIT` |

**Trade-off:** Merge logic must mirror fragment rewrite (`__dc_agg_N` column names). Correctness
is test-driven per query shape.

---

### D4 — Expression aggregates: nullable `inputColumn` + `inputExpression`

**Problem:** TPC-H uses `SUM(l_extendedprice * (1 - l_discount))`, `SUM(CASE WHEN …)`, etc.
`QueryAnalysisExtractor` originally required bare column operands.

**Decision (Phase 1):** Extend `AggregateSpec` with optional `inputExpression`. Analysis never
throws on expressions; fragment SQL preserves the full operand `SqlNode`; merge still uses
`SUM(__dc_agg_N)` over partials.

**Trade-off:** Merge metadata tracks merge column names, not expression trees. Sufficient for
additive aggregates (`SUM`, `COUNT`). Non-decomposable aggregates (`COUNT(DISTINCT)`, median)
need separate strategies (Phase 6).

---

### D5 — Derived tables in `FROM`: recurse, do not flatten

**Problem:** `FROM (SELECT …) AS alias` made `collectTableNames()` return empty and `rewriteFrom()`
skip shard qualification inside the subquery.

**Decision (Phase 2):** Recurse into `SqlSelect` nodes in both collection and rewrite. Preserve
subquery structure; only qualify base tables inside. Support `AS(subquery, alias, col, …)` via
`recreateAsCall()`.

**Trade-off:** Correct for Q07/Q09-style inline views (inner query is relational, outer query
aggregates). Insufficient when inner query has its own `GROUP BY` over cross-shard data (Q13).

**Why Calcite doesn't solve this alone:** Parsing nested `FROM` is easy. Knowing *where* the
subquery may execute relative to shards is a distribution decision — still ours to make.

---

### D6 — ORDER BY on `GROUP BY` queries: defer to coordinator merge

**Problem:** Fragment SQL rewrote `COUNT(*) AS custdist` to `COUNT(*) AS __dc_agg_0`. Appending
outer `ORDER BY custdist` to fragment SQL failed (alias not found). Even when the alias exists,
per-shard ordering is meaningless before global merge.

**Decision:** If `analysis.groupByColumns()` is non-empty, do **not** push `ORDER BY`/`LIMIT` to
fragments. Apply them in `MergeSqlBuilder.buildGroupByMerge(..., topK)` on the coordinator.

**Trade-off:** Slightly more coordinator work; semantically correct. Overlaps with planned
Phase 8 (TOP_K on aggregate aliases) — implemented early because Phase 2 Q13 required it.

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
Merging those histograms with `GROUP BY_MERGE` is wrong — a customer's orders span shards.

**Decision:** Detect `AS(inner Select with GROUP BY, …)` via `NestedDerivedTableDetector`.

1. **Fragments:** execute **inner** `SELECT` only (shard-qualified).
2. **Coordinator phase 1:** `GROUP BY` merge on inner keys → one row per customer with global
   order count (`buildGroupByMerge` with derived column aliases).
3. **Coordinator phase 2:** run **outer** aggregation as local SQL on phase-1 rows
   (`CoordinatorOuterSqlGenerator` — uses `COUNT(*)`, not `SUM(__dc_agg_N)`).

**Trade-off:** Extra merge strategy and coordinator temp-table round trip. Only triggered for
derived-table + inner-`GROUP BY` pattern. A full optimizer would plan this as
`Exchange` → partial agg → `Exchange` → final agg.

---

### D8 — Q08 deferred to Phase 4 (multi-aggregate arithmetic)

**Problem:** `SUM(CASE …) / SUM(volume)` is one SELECT item with arithmetic over two aggregates.
`QueryAnalysisExtractor` inspects top-level select items only.

**Decision:** Do not claim Q08 in Phase 2. Phase 4 will walk expression trees, assign multiple
`__dc_agg_N` columns, and replay arithmetic at merge time.

**Why it looked like Phase 2:** Q08 has a derived table in `FROM` (Phase 2 fixes planning/rewrite
of inner tables) but fails at analysis with "Expected column identifier but found: SUM(…) / SUM(…)".

---

### D9 — Parser edge cases handled in planner, not by "more Calcite"

**Examples:** trailing semicolons (strip in `parse()`), reserved aliases like `value` (Phase 5).

**Decision:** Prefer small normalizations at the DuckCluster boundary over forking Calcite.

**Trade-off:** Masks client SQL quirks; keeps worker/merge SQL clean.

### D10 — Comma-joins, aliases, and tied driving-table selection (Phase 3)

**Problem:** TPC-H uses `FROM a, b, c` comma-joins and `table AS alias`. Table
classification and `rewriteFrom` needed consistent base-table resolution. When two tables
share the same shard count (e.g. `orders` and `lineitem` both have 6), driving-table choice
was non-deterministic.

**Decision:**

1. `TableNameSupport.baseTableName()` unwraps `AS` aliases and `catalog.table` qualifiers.
2. `collectTableNamesRecursive()` uses `TableNameSupport` so aliases like `n1` are not
   mistaken for tables.
3. Tied shard counts: **last table in FROM traversal order** wins (`>=` comparison).
4. `rewriteFrom` preserves aliases on broadcast `UNION ALL` subqueries.
5. `MergeSqlBuilder.buildGroupByMerge` emits columns in **`outputColumnNames` order** (fixes
   Q10 column ordering vs baseline).

**Trade-off:** Tied driving-table choice is heuristic but correct either way when shard counts
match. Explicit JOIN tests updated to expect last-table-wins semantics.

---

## 4. Query status (planner + execution)

Snapshot after Phase 2 work on SF0.01. "Phase" = hardening plan phase that unlocks the query.

| Query | End-to-end | Blocking phase | Primary issue |
|-------|------------|----------------|---------------|
| Q01 | ✅ | 1 | Expression `SUM` |
| Q06 | ✅ | 1 | Expression `SUM(CASE …)` |
| Q07 | ✅ | 2 | Derived table in `FROM` |
| Q09 | ✅ | 2 | Derived table in `FROM` |
| Q13 | ✅ | 2 + D7 | Nested inner `GROUP BY` |
| Q05 | ✅ | 3 | Comma-join six tables |
| Q10 | ✅ | 3 | Comma-join + alias + column order |
| Q12 | ✅ | 3 | `orders, lineitem` comma-join |
| Q08 | ❌ | 4 | Multi-aggregate arithmetic in one SELECT item |
| Q02, Q04, Q15, Q17, Q18, Q20, Q21, Q22 | ❌ | 7 | Correlated / predicate subqueries |
| Q14 | ❌ | 4 | Nested aggregates in expression |
| Q16 | ❌ | 6 | `COUNT(DISTINCT …)` |
| Q11 | ❌ | 5 | Parser reserved word `value` |
| Remaining | ❌ | 3, 9, … | Joins, execution, merge edge cases |

Update this table when integration tests change.

---

## 5. What "Calcite handles it" would actually require

If the goal is for Calcite to own distributed planning end-to-end, the project would need roughly:

1. **`DuckClusterSchema`** — expose each `(table, shardId)` as a partition or custom table scan
2. **SQL → `RelNode`** — `PlannerImpl` with cluster metadata (shard counts, broadcast costs)
3. **Traits** — e.g. `SINGLETON`, `HASH(c_custkey)`, `ROUND_ROBIN`
4. **Rules** — broadcast join, aggregate splitting, exchange insertion
5. **Physical SQL** — `RelToSqlConverter` per fragment with partition pruning
6. **Merge as final `Aggregate`/`Sort`** — or a coordinator `EnumerableRel` implementation

That is a multi-month optimizer project. The current approach trades generality for a **working
TPC-H correctness gate** on a fixed execution model (broadcast + partial agg + DuckDB merge).

**Open question (not decided):** After 22/22 TPC-H pass, do we incrementally move shapes from
hand-rolled `SqlNode` rewrite into Calcite rules, or keep the explicit pipeline and widen
coverage?

---

## 6. Key source files

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

---

## 7. Changelog

| Date | Change |
|------|--------|
| 2026-07-10 | D10 Phase 3: comma-joins, aliases, tied driving-table rule, merge column order |
| 2026-07-10 | Initial doc: Calcite scope clarification, D1–D9, Q-status table |
