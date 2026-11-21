-- Keep both feature paths explicit; the grouping-scope default remains false.
SET spark.sql.analyzer.singlePassResolver.dualRunWithLegacy=false;
CREATE TEMPORARY FUNCTION having_id(v INT) RETURNS INT RETURN v;

SET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled=true;

-- Grouping scope uses shared HAVING placement after lifting chained lateral aliases.
SELECT k, sum(v) AS s, s * 2 AS s2, s2 + 1 AS s3, count(*) OVER () AS n
FROM VALUES ('drop', 2), ('keep', 1) AS t(k, v)
GROUP BY ROLLUP(k) HAVING k = 'keep';

-- QUALIFY resolves nonselected grouping columns after HAVING and lateral-alias lifting.
SELECT sum(v) AS s, s * 2 AS s2, count(*) OVER () AS n
FROM VALUES ('drop', 2), ('keep', 1) AS t(k, v)
GROUP BY ROLLUP(k) HAVING k = 'keep' QUALIFY k = 'keep' AND n > 0;

-- Mixed computed keys and aggregate conjuncts retain the original key mapping.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k + 1) HAVING k + 1 = 3 AND sum(v) > 0;

-- Correlation remaps a complete original key to the nullable aggregate output.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING (SELECT k + 1) >= 3 ORDER BY 1;

-- Explicit grouping_id arguments use the original key identity.
SELECT k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k) HAVING sum(v) > 0 AND grouping_id(k) = 0 ORDER BY k;

-- Computed grouping keys keep their original binding across expansion.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING k + 1 = 3;

-- Computed grouping keys keep their original binding across expansion.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY CUBE(k + 1) HAVING k + 1 = 3;

-- Computed grouping keys keep their original binding across expansion.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY GROUPING SETS ((k + 1)) HAVING k + 1 = 3;

-- Nongrouping aggregate inputs precede SELECT aliases and keep grand-total inputs.
SELECT k, sum(v) AS v, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k) HAVING sum(v) > 0 ORDER BY k;

-- A constituent of a computed key is still an aggregate input, not a SELECT alias.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING sum(k) > 0 ORDER BY k;

-- Nongrouped scalar aliases retain their output types after tentative input resolution.
SELECT max(length(v)) AS v, count(*) OVER () AS n
FROM VALUES (1, 'ab'), (2, 'abcd') AS t(k, v)
GROUP BY ROLLUP(k) HAVING v > 3 ORDER BY 1;

-- HAVING aggregates read original rows, including the grand total's non-null input.
SELECT k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k) HAVING sum(k) > 0 ORDER BY k;

-- Pre-bound HAVING dependencies do not become QUALIFY name candidates.
SELECT k + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k) HAVING k = 2 QUALIFY k > 0;

-- Grouping predicates and aggregate arguments keep distinct input identities.
SELECT k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k) HAVING sum(k) > 0 AND grouping(k) = 0 ORDER BY k;

-- A SQL UDF in HAVING waits for unselected grouping references.
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k HAVING having_id(k) > 1;

-- A SQL UDF in SELECT cannot separate HAVING from its aggregate too early.
SELECT having_id(sum(v)) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k HAVING k > 1;

-- Correlated computed grouping keys are finalized before QUALIFY.
SELECT (g + 1) + 10 AS g, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(g)
GROUP BY g + 1 HAVING (SELECT g + 1) >= 3 QUALIFY n > 0 ORDER BY 1;

-- SQL UDF rewriting preserves complete correlated grouping dependencies.
SELECT (g + 1) + 10 AS g, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(g)
GROUP BY g + 1 HAVING having_id(1) > 0 AND (SELECT g + 1) >= 3 ORDER BY 1;

-- Function grouping keys precede conflicting SELECT aliases.
SELECT concat(lower(name), '!') AS name, count(*) OVER () AS n
FROM VALUES ('Bob'), ('bob'), ('Al') AS t(name)
GROUP BY lower(name) HAVING lower(name) = 'bob';

-- Function arguments retain the same scope for multiple-argument functions.
SELECT concat('!', substr(name, 1, 1)) AS name, count(*) OVER () AS n
FROM VALUES ('ax'), ('ay'), ('bz') AS t(name)
GROUP BY substr(name, 1, 1) HAVING substr(name, 1, 1) = 'a';

-- Implicit casts do not redirect a computed grouping key to a SELECT alias.
SELECT k + 1.5 + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY k + 1.5 HAVING k + 1.5 >= 3 ORDER BY 1;

-- Early-resolved grouping-set predicates filter before windows.
SELECT k, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY GROUPING SETS ((k)) HAVING k = 2;

-- ROLLUP preserves grouping scope through nested windows.
SELECT k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY ROLLUP(k) HAVING k = 2;

-- CUBE preserves grouping scope through nested windows.
SELECT k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY CUBE(k) HAVING k = 2;

-- Generator placement uses the separate nested-window correction.
SELECT explode(array(k)) AS col, k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY k HAVING k = 2;

-- Grouping scope does not make a window alias valid in HAVING.
SELECT k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY GROUPING SETS ((k), ()) HAVING n = 3 ORDER BY k;

-- Unselected grouping_id remains usable in HAVING and ORDER BY.
SELECT k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING sum(v) > 0 AND grouping_id() = 0 ORDER BY grouping_id(), k;

-- Grouping predicates retain their virtual dependency without exposing helper names.
SELECT k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k)
HAVING sum(v) > 0 AND grouping(k) = 0 ORDER BY grouping_id(), k;

SET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled=false;

-- The disabled path retains the same shared lateral-alias placement.
SELECT k, sum(v) AS s, s * 2 AS s2, s2 + 1 AS s3, count(*) OVER () AS n
FROM VALUES ('drop', 2), ('keep', 1) AS t(k, v)
GROUP BY ROLLUP(k) HAVING k = 'keep';

-- The disabled path also resolves QUALIFY after HAVING and lateral-alias lifting.
SELECT sum(v) AS s, s * 2 AS s2, count(*) OVER () AS n
FROM VALUES ('drop', 2), ('keep', 1) AS t(k, v)
GROUP BY ROLLUP(k) HAVING k = 'keep' QUALIFY k = 'keep' AND n > 0;

-- These rollback controls retain legacy rows and analysis errors for correlation,
-- explicit grouping arguments, and QUALIFY. The enabled counterparts above must succeed.
-- Expanded grouping-key names were already valid ORDER BY candidates on the legacy path.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING k + 1 = 3 ORDER BY `(k + 1)`;

-- Mixed computed keys and aggregate conjuncts retain the original key mapping.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k + 1) HAVING k + 1 = 3 AND sum(v) > 0;

-- Correlation remaps a complete original key to the nullable aggregate output.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING (SELECT k + 1) >= 3 ORDER BY 1;

-- Explicit grouping_id arguments use the original key identity.
SELECT k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k) HAVING sum(v) > 0 AND grouping_id(k) = 0 ORDER BY k;

-- Computed grouping keys keep their original binding across expansion.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING k + 1 = 3;

-- Computed grouping keys keep their original binding across expansion.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY CUBE(k + 1) HAVING k + 1 = 3;

-- Computed grouping keys keep their original binding across expansion.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY GROUPING SETS ((k + 1)) HAVING k + 1 = 3;

-- Nongrouping aggregate inputs precede SELECT aliases and keep grand-total inputs.
SELECT k, sum(v) AS v, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k) HAVING sum(v) > 0 ORDER BY k;

-- A constituent of a computed key is still an aggregate input, not a SELECT alias.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING sum(k) > 0 ORDER BY k;

-- Nongrouped scalar aliases retain their output types after tentative input resolution.
SELECT max(length(v)) AS v, count(*) OVER () AS n
FROM VALUES (1, 'ab'), (2, 'abcd') AS t(k, v)
GROUP BY ROLLUP(k) HAVING v > 3 ORDER BY 1;

-- HAVING aggregates read original rows, including the grand total's non-null input.
SELECT k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k) HAVING sum(k) > 0 ORDER BY k;

-- Pre-bound HAVING dependencies do not become QUALIFY name candidates.
SELECT k + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k) HAVING k = 2 QUALIFY k > 0;

-- Grouping predicates and aggregate arguments keep distinct input identities.
SELECT k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k) HAVING sum(k) > 0 AND grouping(k) = 0 ORDER BY k;

-- A SQL UDF in HAVING waits for unselected grouping references.
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k HAVING having_id(k) > 1;

-- A SQL UDF in SELECT cannot separate HAVING from its aggregate too early.
SELECT having_id(sum(v)) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k HAVING k > 1;

-- Correlated computed grouping keys are finalized before QUALIFY.
SELECT (g + 1) + 10 AS g, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(g)
GROUP BY g + 1 HAVING (SELECT g + 1) >= 3 QUALIFY n > 0 ORDER BY 1;

-- SQL UDF rewriting preserves complete correlated grouping dependencies.
SELECT (g + 1) + 10 AS g, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(g)
GROUP BY g + 1 HAVING having_id(1) > 0 AND (SELECT g + 1) >= 3 ORDER BY 1;

-- Function grouping keys precede conflicting SELECT aliases.
SELECT concat(lower(name), '!') AS name, count(*) OVER () AS n
FROM VALUES ('Bob'), ('bob'), ('Al') AS t(name)
GROUP BY lower(name) HAVING lower(name) = 'bob';

-- Function arguments retain the same scope for multiple-argument functions.
SELECT concat('!', substr(name, 1, 1)) AS name, count(*) OVER () AS n
FROM VALUES ('ax'), ('ay'), ('bz') AS t(name)
GROUP BY substr(name, 1, 1) HAVING substr(name, 1, 1) = 'a';

-- Implicit casts do not redirect a computed grouping key to a SELECT alias.
SELECT k + 1.5 + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY k + 1.5 HAVING k + 1.5 >= 3 ORDER BY 1;

-- Early-resolved grouping-set predicates filter before windows.
SELECT k, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY GROUPING SETS ((k)) HAVING k = 2;

-- ROLLUP preserves grouping scope through nested windows.
SELECT k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY ROLLUP(k) HAVING k = 2;

-- CUBE preserves grouping scope through nested windows.
SELECT k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY CUBE(k) HAVING k = 2;

-- Generator placement uses the separate nested-window correction.
SELECT explode(array(k)) AS col, k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY k HAVING k = 2;

-- Grouping scope does not make a window alias valid in HAVING.
SELECT k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY GROUPING SETS ((k), ()) HAVING n = 3 ORDER BY k;

-- Unselected grouping_id remains usable in HAVING and ORDER BY.
SELECT k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY GROUPING SETS ((k), ())
HAVING sum(v) > 0 AND grouping_id() = 0 ORDER BY grouping_id(), k;

-- The legacy path rejects grouping(k) with GROUPING_COLUMN_MISMATCH; keep that rollback
-- behavior explicit while the enabled case above checks the valid grouping predicate.
SELECT k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY ROLLUP(k)
HAVING sum(v) > 0 AND grouping(k) = 0 ORDER BY grouping_id(), k;

-- The separate window-order rollback remains effective with either grouping-scope value.
SET spark.sql.legacy.evaluateHavingAfterInnerWindow=true;
SELECT k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY k HAVING k = 2;
SET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled=true;
SELECT k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY k HAVING k = 2;
RESET spark.sql.legacy.evaluateHavingAfterInnerWindow;

-- Correlated function keys use the same complete-key probe and outer dependency remap.
SELECT concat(lower(name), '!') AS name, count(*) OVER () AS n
FROM VALUES ('Bob'), ('bob'), ('Al') AS t(name)
GROUP BY lower(name) HAVING (SELECT lower(name)) = 'bob';

-- A failed input-key probe must discard its bindings before resolving a string alias.
SELECT max(name) AS name, count(*) OVER () AS n
FROM VALUES (1, 'Bob'), (1, 'bob'), (2, 'Al') AS t(k, name)
GROUP BY k HAVING lower(name) = 'bob';

-- Existing expanded grouping-key names keep their ORDER BY binding.
SELECT (k + 1) + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY ROLLUP(k + 1) HAVING k + 1 = 3 ORDER BY `(k + 1)`;

-- Non-ANSI coercion follows the active session too.
SET spark.sql.ansi.enabled=false;
SELECT k + 1.5 + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY k + 1.5 HAVING k + 1.5 >= 3 ORDER BY 1;
RESET spark.sql.ansi.enabled;

DROP TEMPORARY FUNCTION having_id;
RESET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled;
RESET spark.sql.analyzer.singlePassResolver.dualRunWithLegacy;

-- Resetting the session restores the default-off grouping scope.
SET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled;
