CREATE TEMPORARY VIEW grouping AS SELECT * FROM VALUES
  ("1", "2", "3", 1),
  ("4", "5", "6", 1),
  ("7", "8", "9", 1)
  as grouping(a, b, c, d);

-- SPARK-17849: grouping set throws NPE #1
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS (());

-- SPARK-17849: grouping set throws NPE #2
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((a));

-- SPARK-17849: grouping set throws NPE #3
SELECT a, b, c, count(d) FROM grouping GROUP BY a, b, c GROUPING SETS ((c));

-- SPARK-52007: grouping set doesn't produce expression IDs in grouping expressions
SELECT a, b, c, d FROM grouping GROUP BY GROUPING SETS (a, b, c, d);
SELECT a, b FROM grouping GROUP BY GROUPING SETS (a, b, d + 1) ORDER BY `(d + 1)`;

-- Group sets without explicit group by
SELECT c1, sum(c2) FROM (VALUES ('x', 10, 0), ('y', 20, 0)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1);

-- Group sets without group by and with grouping
SELECT c1, sum(c2), grouping(c1) FROM (VALUES ('x', 10, 0), ('y', 20, 0)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1);

-- Mutiple grouping within a grouping set
SELECT c1, c2, Sum(c3), grouping__id
FROM   (VALUES ('x', 'a', 10), ('y', 'b', 20) ) AS t (c1, c2, c3)
GROUP  BY GROUPING SETS ( ( c1 ), ( c2 ) )
HAVING GROUPING__ID > 1;

-- Group sets without explicit group by
SELECT grouping(c1) FROM (VALUES ('x', 'a', 10), ('y', 'b', 20)) AS t (c1, c2, c3) GROUP BY GROUPING SETS (c1,c2);

-- Mutiple grouping within a grouping set
SELECT -c1 AS c1 FROM (values (1,2), (3,2)) t(c1, c2) GROUP BY GROUPING SETS ((c1), (c1, c2));

-- complex expression in grouping sets
SELECT a + b, b, sum(c) FROM (VALUES (1,1,1),(2,2,2)) AS t(a,b,c) GROUP BY GROUPING SETS ( (a + b), (b));

-- complex expression in grouping sets
SELECT a + b, b, sum(c) FROM (VALUES (1,1,1),(2,2,2)) AS t(a,b,c) GROUP BY GROUPING SETS ( (a + b), (b + a), (b));

-- more query constructs with grouping sets
SELECT c1 AS col1, c2 AS col2
FROM   (VALUES (1, 2), (3, 2)) t(c1, c2)
GROUP  BY GROUPING SETS ( ( c1 ), ( c1, c2 ) )
HAVING col2 IS NOT NULL
ORDER  BY -col1;

-- negative tests - must have at least one grouping expression
SELECT a, b, c, count(d) FROM grouping GROUP BY WITH ROLLUP;

SELECT a, b, c, count(d) FROM grouping GROUP BY WITH CUBE;

SELECT c1 FROM (values (1,2), (3,2)) t(c1, c2) GROUP BY GROUPING SETS (());

-- GROUP BY GROUPING SETS (()) is a grand total and must return one row over empty input, just
-- like an aggregation without a GROUP BY clause.
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) WHERE k > 100 GROUP BY GROUPING SETS (());

-- Semantically identical query without GROUP BY, for comparison.
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) WHERE k > 100;

-- Grand total over empty input with grouping_id() in the SELECT list.
SELECT count(*) AS c, grouping_id() AS g
FROM VALUES (1), (2), (3) AS t(k) WHERE k > 100 GROUP BY GROUPING SETS (());

-- Grand total over non-empty input still returns the single aggregated row.
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS (());

-- Meaningful aggregates make the grand-total row explicit. Over empty input it is a single row of
-- NULL measures with count 0 (identical to the same query with no GROUP BY); over non-empty input
-- it carries the real totals.
SELECT sum(v) AS total, avg(v) AS mean, max(v) AS hi, count(*) AS c
FROM VALUES (10), (20), (30) AS t(v) WHERE v > 100 GROUP BY GROUPING SETS (());

SELECT sum(v) AS total, avg(v) AS mean, max(v) AS hi, count(*) AS c
FROM VALUES (10), (20), (30) AS t(v) WHERE v > 100;

SELECT sum(v) AS total, avg(v) AS mean, max(v) AS hi, count(*) AS c
FROM VALUES (10), (20), (30) AS t(v) GROUP BY GROUPING SETS (());

-- Contrast: grouping by a real column over empty input returns no rows (no groups), whereas the
-- grand total above returns one row -- the grand-total-specific semantics this fix restores.
SELECT v, count(*) AS c FROM VALUES (10), (20), (30) AS t(v) WHERE v > 100 GROUP BY v;

-- grouping_id() over a grand total is 0, so it resolves in HAVING and ORDER BY.
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS (()) HAVING grouping_id() = 0;
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS (()) ORDER BY grouping_id();

-- grouping() over a grand total references a non-grouping column and is rejected.
SELECT grouping(k) FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS (());

-- A grouping function over a plain aggregate with no GROUP BY clause (and no grouping set) is
-- still rejected, in HAVING and in ORDER BY -- the grand-total lowering must not make these fold.
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) HAVING grouping_id() = 0;
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) ORDER BY grouping_id();
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) HAVING grouping(k) = 0;
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) ORDER BY grouping(k);

-- GROUP BY GROUPING SETS ((), ()) is a *duplicated* empty grouping set: it is NOT the single-set
-- grand total, so it stays on the Expand path (with a spark_grouping_id key plus a
-- _gen_grouping_pos dedup key) and is not lowered to a global aggregate. It emits one row per
-- empty set (two rows), each with grouping_id() = 0 in the SELECT list. In HAVING/ORDER BY a
-- grouping function still errors (the last grouping key is _gen_grouping_pos, not
-- spark_grouping_id), and grouping() over a non-grouping column is rejected. This is unchanged by
-- the grand-total lowering.
SELECT count(*) AS c, grouping_id() AS g
FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS ((), ());
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS ((), ()) HAVING grouping_id() = 0;
SELECT count(*) AS c FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS ((), ()) ORDER BY grouping_id();
SELECT grouping(k) FROM VALUES (1), (2), (3) AS t(k) GROUP BY GROUPING SETS ((), ());

-- Grand total over a named relation rather than inline data in the query: the grand total reads
-- FROM a temporary view.
CREATE TEMPORARY VIEW grouping_grand_total AS
  SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(k, v);

-- Empty input (filter removes all rows): one grand-total row of NULL measures with count 0.
SELECT sum(v) AS total, avg(v) AS mean, max(v) AS hi, count(*) AS c
FROM grouping_grand_total WHERE k > 100 GROUP BY GROUPING SETS (());

-- Non-empty input: the real totals in a single grand-total row.
SELECT sum(v) AS total, avg(v) AS mean, max(v) AS hi, count(*) AS c
FROM grouping_grand_total GROUP BY GROUPING SETS (());

-- grouping_id() over the grand total folds to 0.
SELECT count(*) AS c, grouping_id() AS g
FROM grouping_grand_total WHERE k > 100 GROUP BY GROUPING SETS (());

DROP VIEW grouping_grand_total;

-- duplicate entries in grouping sets
SELECT k1, k2, avg(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

SELECT grouping__id, k1, k2, avg(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

SELECT grouping(k1), k1, k2, avg(v) FROM (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY GROUPING SETS ((k1),(k1,k2),(k2,k1));

-- grouping_id function
SELECT grouping_id(k1, k2), avg(v) from (VALUES (1,1,1),(2,2,2)) AS t(k1,k2,v) GROUP BY k1, k2 GROUPING SETS ((k2, k1), k1);

SELECT CASE WHEN a IS NULL THEN count(b) WHEN b IS NULL THEN count(c) END
FROM grouping
GROUP BY GROUPING SETS (a, b, c);
