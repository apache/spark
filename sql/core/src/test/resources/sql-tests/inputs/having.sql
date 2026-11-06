create temporary view hav as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3),
  ("one", 5)
  as hav(k, v);

-- having clause
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2;

-- having condition contains grouping column
SELECT count(k) FROM hav GROUP BY v + 1 HAVING v + 1 = 2;

-- invalid having condition contains grouping column
SELECT count(k) FROM hav GROUP BY v HAVING v = array(1);

-- SPARK-11032: resolve having correctly
SELECT MIN(t.v) FROM (SELECT * FROM hav WHERE v > 0) t HAVING(COUNT(1) > 0);

-- SPARK-20329: make sure we handle timezones correctly
SELECT a + b FROM VALUES (1L, 2), (3L, 4) AS T(a, b) GROUP BY a + b HAVING a + b > 1;

-- SPARK-31519: Cast in having aggregate expressions returns the wrong result
SELECT SUM(a) AS b, CAST('2020-01-01' AS DATE) AS fake FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY b HAVING b > 10;

-- SPARK-31663: Grouping sets with having clause returns the wrong result
SELECT SUM(a) AS b FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY GROUPING SETS ((b), (a, b)) HAVING b > 10;
SELECT SUM(a) AS b FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY CUBE(a, b) HAVING b > 10;
SELECT SUM(a) AS b FROM VALUES (1, 10), (2, 20) AS T(a, b) GROUP BY ROLLUP(a, b) HAVING b > 10;

-- SPARK-33131: Grouping sets with having clause can not resolve qualified col name.
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY GROUPING SETS(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY CUBE(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY ROLLUP(t.c1) HAVING t.c1 = 1;
SELECT c1 FROM VALUES (1, 2) as t(c1, c2) GROUP BY t.c1 HAVING t.c1 = 1;

-- SPARK-28386: Resolve ORDER BY agg function with HAVING clause, while the agg function presents on SELECT list
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2 ORDER BY sum(v);

-- SPARK-28386: Resolve ORDER BY agg function with HAVING clause, while the agg function does not present on SELECT list
SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2 ORDER BY avg(v);

-- SPARK-52385: Remove TempResolvedColumns from InheritAnalysisRules name
SELECT sum(v) FROM hav HAVING avg(try_add(v, 1)) = 1;
SELECT sum(v) FROM hav HAVING sum(try_add(v, 1)) = 1;
SELECT sum(v) FROM hav HAVING sum(ifnull(v, 1)) = 1;
SELECT sum(v) FROM hav GROUP BY ALL HAVING sum(ifnull(v, 1)) = 1;
SELECT sum(v) FROM hav GROUP BY v HAVING sum(ifnull(v, 1)) = 1;
SELECT v + 1 FROM hav GROUP BY ALL HAVING avg(try_add(v, 1)) = 1;
SELECT v + 1 FROM hav GROUP BY ALL HAVING avg(try_add(v, 1) + 1) = 1;
SELECT sum(v) FROM hav GROUP BY ifnull(v, 1) + 1 order by ifnull(v, 1) + 1;

-- HAVING condition should prefer table columns over aliases
SELECT 1 AS `2`, 2 FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;
SELECT 2, 1 AS `2` FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;
SELECT 1 AS `2` FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;
SELECT 2 FROM VALUES (2) t (`2`) GROUP BY `2` HAVING `2` > 2;

-- HAVING condition is resolved as a semantically equivalent to one in the SELECT list
SELECT SUM(v) + 1 FROM hav HAVING SUM(v) + 1;
SELECT 1 + SUM(v) FROM hav HAVING SUM(v) + 1;
SELECT SUM(v) + 1 FROM hav HAVING 1 + SUM(v);
SELECT MAX(v) + SUM(v) FROM hav HAVING SUM(v) + MAX(v);
SELECT SUM(v) + 1 + MIN(v) FROM hav HAVING 1 + 1 + 1 + MIN(v) + 1 + SUM(v);

-- HAVING with outer reference to alias in outer project list
SELECT col1 AS alias
FROM values(1)
GROUP BY col1
HAVING (
    SELECT col1 = 1
);

SELECT col1 AS alias
FROM values(named_struct('a', 1))
GROUP BY col1
HAVING (
    SELECT col1.a = 1
);

SELECT col1 AS alias
FROM values(array(1))
GROUP BY col1
HAVING (
    SELECT col1[0] = 1
);

SELECT col1 AS alias
FROM values(map('a', 1))
GROUP BY col1
HAVING (
    SELECT col1[0] = 1
);

-- Missing attribute (col2) in HAVING is added only once
SELECT col1 FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 = col2;
SELECT col1 AS a, a AS b FROM VALUES(1,2) GROUP BY col1, col2 HAVING col2 = col2;

-- Replacing Having condition with alias from below
SELECT col1, col1 AS a FROM VALUES(1) GROUP BY col1 HAVING col1 > 0;
SELECT col1 AS a, col1 FROM VALUES(1) GROUP BY col1 HAVING col1 > 0;
SELECT make_date(col1, col2, col3) AS a, a AS b FROM VALUES(1,2,3) GROUP BY make_date(col1, col2, col3) HAVING make_date(col1, col2, col3) > '2025-01-01';
SELECT 1 AS a, 1 / a AS b, ZEROIFNULL(SUM(col1)) FROM VALUES(1) GROUP BY 1 HAVING ZEROIFNULL(SUM(col1)) > 0;
SELECT col1 AS a, SUM(col2) AS b, CASE WHEN col1 = 1 THEN 1 END AS c FROM VALUES(1,2) GROUP BY col1 HAVING CASE WHEN col1 = 1 THEN 1 END = 1;

-- Deduplicate expressions before adding them to Aggregate
SELECT col1 FROM VALUES(1,2) GROUP BY col1 HAVING MAX(col2) == (SELECT 1 WHERE MAX(col2) = 1);
SELECT col1 FROM VALUES(1,2) GROUP BY col1 HAVING (SELECT 1 WHERE MAX(col2) = 1) == MAX(col2);
SELECT col1 FROM VALUES(1,2) GROUP BY col1 HAVING (SELECT 1 WHERE MAX(col2) = 1) == (SELECT 1 WHERE MAX(col2) = 1);
SELECT col1 FROM VALUES(1,2) GROUP BY col1 HAVING bool_or(col2 = 1) AND bool_or(col2 = 1);
SELECT 1 GROUP BY COALESCE(1, 1) HAVING COALESCE(1, 1) = 1  OR COALESCE(1, 1) IS NOT NULL;
SELECT col1 FROM VALUES (1) t1 GROUP BY col1 HAVING (
    SELECT MAX(t2.col1) FROM VALUES (1) t2 WHERE t2.col1 == MAX(t1.col1) GROUP BY t2.col1 HAVING (
        SELECT t3.col1 FROM VALUES (1) t3 WHERE t3.col1 == MAX(t2.col1)
    )
);

-- HAVING name resolution should prioritize table columns when referencing outer ones
CREATE TEMPORARY VIEW hav_outer AS SELECT * FROM VALUES (1, 2), (3, 4) AS hav_outer(col1, col2);
SELECT col2 FROM hav_outer GROUP BY col1, col2 HAVING (SELECT col1);
-- Outer reference in a comparison, in EXISTS and IN subqueries, and under an aggregate.
SELECT col2 FROM hav_outer GROUP BY col1, col2 HAVING (SELECT col1) > 1;
SELECT col2 FROM hav_outer GROUP BY col1, col2 HAVING EXISTS (SELECT col1);
SELECT col2 FROM hav_outer GROUP BY col1, col2 HAVING col2 IN (SELECT col1 + 1);
SELECT col2 FROM hav_outer GROUP BY col2 HAVING (SELECT max(col1)) > 1;
-- The referenced table column has to be a grouping column.
SELECT col2 FROM hav_outer GROUP BY col2 HAVING (SELECT col1);

DROP VIEW hav_outer;

-- HAVING keeps grouping scope below a window; aggregate aliases keep their types.
SET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled=true;
SELECT k + 10 AS k, avg(score) AS score, count(*) OVER () AS n
FROM VALUES (1, 2), (1, 3), (2, 2), (2, 3) AS t(k, score)
GROUP BY k HAVING k >= 2 AND score >= 2.5 ORDER BY 1;

-- Ambiguous grouping names fall back to aliases; computed grouping inputs retain scope.
SELECT sum(1) AS k, (g + 1) + 10 AS g, count(*) OVER () AS n
FROM VALUES (1, 1), (1, 2) AS t1(k, g) CROSS JOIN VALUES (2) AS t2(k)
GROUP BY t1.k, t2.k, g + 1 HAVING k >= 1 AND g + 1 >= 3 ORDER BY g;

-- Nested identity calls delay expansion so the lambda capture resolves after window extraction.
CREATE TEMPORARY FUNCTION having_id(v INT) RETURNS INT RETURN v;
SELECT sum(k) AS k, sum(1) AS a, count(*) OVER () AS n
FROM (
  SELECT having_id(having_id(having_id(k))) AS k FROM VALUES (1), (1), (2) AS t(k)
) AS s
GROUP BY k HAVING exists(array(a), x -> k >= 2) ORDER BY a, k;
DROP TEMPORARY FUNCTION having_id;

-- Correlated references resolve in HAVING grouping scope.
SELECT sum(k) AS k, count(*) OVER () AS n
FROM VALUES (1), (1), (2) AS t(k)
GROUP BY k HAVING (SELECT k) >= 2 ORDER BY k;

-- Keep complete computed keys as correlated dependencies, rather than bare input columns.
SELECT (g + 1) + 10 AS g, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(g)
GROUP BY g + 1 HAVING (SELECT g + 1) >= 3 ORDER BY 1;

-- A nested grouping field takes precedence over a nested SELECT-alias field.
SELECT named_struct('x', s.x + 10) AS s, count(*) OVER () AS n
FROM VALUES (named_struct('x', 1)), (named_struct('x', 2)) AS t(s)
GROUP BY s.x HAVING s.x >= 2 ORDER BY s.x;

-- A raw input of a computed grouping expression must not mask an aggregate alias.
SELECT sum(v) AS k, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k + 1 HAVING k >= 15 ORDER BY 1;

-- Ungrouped inputs still participate in detecting ambiguous names before alias fallback.
SELECT sum(1) AS k, sum(1) AS a, count(*) OVER () AS n
FROM VALUES (1), (2) AS t1(k) CROSS JOIN VALUES (9) AS t2(k)
GROUP BY t1.k HAVING exists(array(a), x -> k >= 2) ORDER BY 1;

-- Aggregate arguments still refer to inputs, even when the grouping key has an output alias.
SELECT k AS a, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY k HAVING sum(k) >= 2 ORDER BY a;

-- Outer aggregate arguments retain their input even when a SELECT alias comes first.
SELECT k AS a, k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY k HAVING EXISTS (
  SELECT 1 FROM VALUES (0) AS u(v) WHERE sum(k) >= 2
) ORDER BY a;

-- A complete computed key inside an outer aggregate must retain its input expression.
SELECT k, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY k, k + 1 HAVING EXISTS (
  SELECT 1 FROM VALUES (0) AS u(v) WHERE sum(k + 1) >= 3
) ORDER BY k;

-- A helper for one grouping input must not hide ambiguity when there is no SELECT alias.
SELECT sum(1) AS a, count(*) OVER () AS n
FROM VALUES (1), (2) AS t1(k) CROSS JOIN VALUES (9) AS t2(k)
GROUP BY t1.k HAVING k >= 2 ORDER BY a;

-- A qualified reference must not make a later ambiguous unqualified reference valid.
SELECT sum(1) AS a, count(*) OVER () AS n
FROM VALUES (1), (2) AS t1(k) CROSS JOIN VALUES (9) AS t2(k)
GROUP BY t1.k HAVING t1.k >= 1 AND k >= 2 ORDER BY a;

-- Nested windows must retain HAVING grouping scope before either window runs.
SELECT k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY k HAVING k = 2 ORDER BY 1;

SELECT k + 10 AS k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY k HAVING k = 2 ORDER BY 1;

-- QUALIFY sees SELECT aliases, not hidden HAVING grouping keys.
SELECT k + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2), (3) AS t(k)
GROUP BY k HAVING k >= 2 QUALIFY k > 12 ORDER BY 1;

-- QUALIFY can resolve unselected grouping keys without binding synthetic helper identities.
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k HAVING k > 0 QUALIFY k = 2 ORDER BY 1;

SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k HAVING k > 0 QUALIFY t.k = 2 ORDER BY 1;

-- Nested-field lookup must also resolve the grouping input before mapping it to a helper.
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (named_struct('x', 1), 10), (named_struct('x', 2), 20) AS t(k, v)
GROUP BY k HAVING k.x > 0 QUALIFY k.x = 2 ORDER BY 1;

-- Descendant ORDER BY lookup cannot make synthetic grouping names visible.
SELECT sum(v) AS a, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k + 1 HAVING sum(v) > 0 ORDER BY `(k + 1)`;

-- A field error on a synthetic name must not expose the helper's type either.
SELECT sum(v) AS a, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k + 1 HAVING sum(v) > 0 ORDER BY `(k + 1)`.x;

-- NULL field extraction must not erase a hidden grouping helper's identity.
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (10), (20) AS t(v)
GROUP BY NULL HAVING sum(v) > 0 ORDER BY `NULL`.x;

-- A real NULL output still permits field extraction when other grouping helpers exist.
SELECT NULL AS k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (10), (20) AS t(v)
GROUP BY v HAVING sum(v) > 0 ORDER BY k.x, s;

-- Error suggestions within HAVING must not expose hidden grouping keys.
SELECT sum(v) AS a, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k + 1 HAVING missing > 0 ORDER BY 1;

-- Error suggestions above HAVING must not expose hidden grouping keys.
SELECT k + 10 AS a, count(*) OVER () AS n
FROM VALUES (1), (2) AS t(k)
GROUP BY k HAVING k >= 2 QUALIFY missing > 0 ORDER BY 1;

-- Disabling the fix preserves the legacy SELECT-alias precedence and window count.
SET spark.sql.analyzer.singlePassResolver.dualRunWithLegacy=false;
SET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled=false;
SELECT k + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2), (3) AS t(k)
GROUP BY k HAVING k >= 2 ORDER BY 1;
-- Rollback also retains the previous nested-window and QUALIFY behavior.
SELECT k + 10 AS k, sum(rank() OVER (ORDER BY sum(v))) OVER () AS n
FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v)
GROUP BY k HAVING k = 2 ORDER BY 1;
SELECT k + 10 AS k, count(*) OVER () AS n
FROM VALUES (1), (2), (3) AS t(k)
GROUP BY k HAVING k >= 2 QUALIFY k > 12 ORDER BY 1;
-- Unselected grouping keys remain available in QUALIFY with the fix disabled.
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (1, 10), (2, 20) AS t(k, v)
GROUP BY k HAVING k > 0 QUALIFY k = 2 ORDER BY 1;
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (named_struct('x', 1), 10), (named_struct('x', 2), 20) AS t(k, v)
GROUP BY k HAVING k.x > 0 QUALIFY k.x = 2 ORDER BY 1;
-- Rollback rejects the same synthetic NULL name and retains real NULL extraction.
SELECT sum(v) AS s, count(*) OVER () AS n
FROM VALUES (10), (20) AS t(v)
GROUP BY NULL HAVING sum(v) > 0 ORDER BY `NULL`.x;
SELECT NULL AS k, sum(v) AS s, count(*) OVER () AS n
FROM VALUES (10), (20) AS t(v)
GROUP BY v HAVING sum(v) > 0 ORDER BY k.x, s;
RESET spark.sql.analyzer.havingWithWindowResolvesGroupingColumnFirst.enabled;
RESET spark.sql.analyzer.singlePassResolver.dualRunWithLegacy;
