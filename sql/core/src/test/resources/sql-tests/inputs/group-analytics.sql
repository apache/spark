CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)
AS testData(a, b);

-- CUBE on overlapping columns
SELECT a + b, b, SUM(a - b) FROM testData GROUP BY a + b, b WITH CUBE;

SELECT a, b, SUM(b) FROM testData GROUP BY a, b WITH CUBE;

-- ROLLUP on overlapping columns
SELECT a + b, b, SUM(a - b) FROM testData GROUP BY a + b, b WITH ROLLUP;

SELECT a, b, SUM(b) FROM testData GROUP BY a, b WITH ROLLUP;

CREATE OR REPLACE TEMPORARY VIEW courseSales AS SELECT * FROM VALUES
("dotNET", 2012, 10000), ("Java", 2012, 20000), ("dotNET", 2012, 5000), ("dotNET", 2013, 48000), ("Java", 2013, 30000)
AS courseSales(course, year, earnings);

-- ROLLUP
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year, (course, year)) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year, (course, year), ()) ORDER BY course, year;

-- CUBE
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year, (course, year)) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year, (course, year), ()) ORDER BY course, year;

-- GROUPING SETS
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course, year);
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course, year, ());
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course);
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(year);

-- Partial ROLLUP/CUBE/GROUPING SETS
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, CUBE(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year), ROLLUP(course, year) ORDER BY course, year;
SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year), ROLLUP(course, year), GROUPING SETS(course, year) ORDER BY course, year;

-- GROUPING SETS with aggregate functions containing groupBy columns
SELECT course, SUM(earnings) AS sum FROM courseSales
GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY course, sum;
SELECT course, SUM(earnings) AS sum, GROUPING_ID(course, earnings) FROM courseSales
GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY course, sum;

-- GROUPING/GROUPING_ID
SELECT course, year, GROUPING(course), GROUPING(year), GROUPING_ID(course, year) FROM courseSales
GROUP BY CUBE(course, year);
SELECT course, year, GROUPING(course) FROM courseSales GROUP BY course, year;
SELECT course, year, GROUPING_ID(course, year) FROM courseSales GROUP BY course, year;
SELECT course, year, grouping__id FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, course, year;

-- GROUPING/GROUPING_ID in having clause
SELECT course, year FROM courseSales GROUP BY CUBE(course, year)
HAVING GROUPING(year) = 1 AND GROUPING_ID(course, year) > 0 ORDER BY course, year;
SELECT course, year FROM courseSales GROUP BY course, year HAVING GROUPING(course) > 0;
SELECT course, year FROM courseSales GROUP BY course, year HAVING GROUPING_ID(course) > 0;
SELECT course, year FROM courseSales GROUP BY CUBE(course, year) HAVING grouping__id > 0;

-- GROUPING/GROUPING_ID in orderBy clause
SELECT course, year, GROUPING(course), GROUPING(year) FROM courseSales GROUP BY CUBE(course, year)
ORDER BY GROUPING(course), GROUPING(year), course, year;
SELECT course, year, GROUPING_ID(course, year) FROM courseSales GROUP BY CUBE(course, year)
ORDER BY GROUPING(course), GROUPING(year), course, year;
SELECT course, year FROM courseSales GROUP BY course, year ORDER BY GROUPING(course);
SELECT course, year FROM courseSales GROUP BY course, year ORDER BY GROUPING_ID(course);
SELECT course, year FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, course, year;

-- Aliases in SELECT could be used in ROLLUP/CUBE/GROUPING SETS
SELECT a + b AS k1, b AS k2, SUM(a - b) FROM testData GROUP BY CUBE(k1, k2);
SELECT a + b AS k, b, SUM(a - b) FROM testData GROUP BY ROLLUP(k, b);
SELECT a + b, b AS k, SUM(a - b) FROM testData GROUP BY a + b, k GROUPING SETS(k);

-- GROUP BY use mixed Separate columns and CUBE/ROLLUP/Gr
SELECT a, b, count(1) FROM testData GROUP BY a, b, CUBE(a, b);
SELECT a, b, count(1) FROM testData GROUP BY a, b, ROLLUP(a, b);
SELECT a, b, count(1) FROM testData GROUP BY CUBE(a, b), ROLLUP(a, b);
SELECT a, b, count(1) FROM testData GROUP BY a, CUBE(a, b), ROLLUP(b);
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b), (a), ());
SELECT a, b, count(1) FROM testData GROUP BY a, CUBE(a, b), GROUPING SETS((a, b), (a), ());
SELECT a, b, count(1) FROM testData GROUP BY a, CUBE(a, b), ROLLUP(a, b), GROUPING SETS((a, b), (a), ());

-- Support nested CUBE/ROLLUP/GROUPING SETS in GROUPING SETS
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(ROLLUP(a, b));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(GROUPING SETS((a, b), (a), ()));

SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b), GROUPING SETS(ROLLUP(a, b)));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b, a, b), (a, b, a), (a, b));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(GROUPING SETS((a, b, a, b), (a, b, a), (a, b)));

SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(ROLLUP(a, b), CUBE(a, b));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS(GROUPING SETS((a, b), (a), ()), GROUPING SETS((a, b), (a), (b), ()));
SELECT a, b, count(1) FROM testData GROUP BY a, GROUPING SETS((a, b), (a), (), (a, b), (a), (b), ());

-- Additional grouping analytics coverage focused on combinations not exercised above:
-- aggregate functions in HAVING / ORDER BY over grouping analytics, lateral column
-- aliases over grouping functions, struct field access inside aggregates, uncorrelated
-- subqueries combined with grouping analytics, and negative cases.

CREATE TEMPORARY VIEW gidSrc AS SELECT * FROM VALUES
  (1, 10), (2, 20), (1, 30)
  AS gidSrc(col1, col2);

CREATE TEMPORARY VIEW aggSrc AS SELECT * FROM VALUES
  (1, 10), (2, 20), (1, 30), (2, 40)
  AS aggSrc(a, b);

CREATE TEMPORARY VIEW pairAb AS SELECT * FROM VALUES
  (1, 10), (2, 20)
  AS pairAb(a, b);

CREATE TEMPORARY VIEW qualSrc AS SELECT * FROM VALUES
  (1, 2)
  AS qualSrc(c1, c2);

CREATE TEMPORARY VIEW triStr AS SELECT * FROM VALUES
  (1, 'a', 10), (1, 'b', 20), (2, 'a', 30)
  AS triStr(col1, col2, col3);

CREATE TEMPORARY VIEW subOuter3 AS SELECT * FROM VALUES
  (1, 10), (1, 20), (2, 30)
  AS subOuter3(col1, col2);

CREATE TEMPORARY VIEW subOuter4 AS SELECT * FROM VALUES
  (1, 10), (1, 20), (2, 30), (3, 40)
  AS subOuter4(col1, col2);

CREATE TEMPORARY VIEW notInOuter AS SELECT * FROM VALUES
  (1, 10), (2, 20), (3, 30)
  AS notInOuter(col1, col2);

CREATE TEMPORARY VIEW exprOuter AS SELECT * FROM VALUES
  (1, 10, 100), (1, 10, 200), (2, 30, 300)
  AS exprOuter(col1, col2, col3);

CREATE TEMPORARY VIEW cubeStr AS SELECT * FROM VALUES
  (1, 'a', 10), (2, 'b', 20)
  AS cubeStr(col1, col2, col3);

CREATE TEMPORARY VIEW localOuter AS SELECT * FROM VALUES (10), (20) AS localOuter(col2);

CREATE TEMPORARY VIEW sub12 AS SELECT * FROM VALUES (1), (2) AS sub12(x);

CREATE TEMPORARY VIEW sub13 AS SELECT * FROM VALUES (1), (3) AS sub13(x);

CREATE TEMPORARY VIEW sub1132 AS SELECT * FROM VALUES (11), (32) AS sub1132(x);

CREATE TEMPORARY VIEW one3 AS SELECT * FROM VALUES (3) AS one3(x);

CREATE TEMPORARY VIEW maxW AS SELECT * FROM VALUES (10), (20) AS maxW(y);

CREATE TEMPORARY VIEW zeroDummy AS SELECT * FROM VALUES (0) AS zeroDummy(dummy);

CREATE TEMPORARY VIEW nums3 AS SELECT * FROM VALUES (1), (2), (3) AS nums3(x);

CREATE TEMPORARY VIEW wide34 AS SELECT * FROM VALUES
  (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34)
  AS wide34(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,
            c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34);

CREATE TEMPORARY VIEW structThree AS SELECT * FROM VALUES
  ('a', 'b', named_struct('val', CAST(1 AS BIGINT)), named_struct('val', 1.0), named_struct('val', 2.0))
  AS structThree(col1, col2, s1, s2, s3);

CREATE TEMPORARY VIEW structOne AS SELECT * FROM VALUES
  ('a', 'b', named_struct('val', CAST(1 AS BIGINT)))
  AS structOne(col1, col2, s1);

CREATE TEMPORARY VIEW structNested AS SELECT * FROM VALUES
  ('a', named_struct('inner', named_struct('val', CAST(1 AS BIGINT))))
  AS structNested(col1, s1);


-- No-argument grouping_id() function (distinct from GROUPING_ID(col, ...) and grouping__id)
SELECT col1, grouping_id()
FROM gidSrc
GROUP BY col1 WITH ROLLUP
ORDER BY col1;

SELECT col1, col2, grouping_id()
FROM gidSrc
GROUP BY GROUPING SETS ((col1, col2), (col1), ())
ORDER BY col1, col2;


-- Lateral column aliases that reference grouping() / grouping_id() results
SELECT col1, grouping_id() AS gid, gid + 1 AS gid_plus
FROM gidSrc
GROUP BY col1 WITH ROLLUP
ORDER BY col1;

SELECT col1, grouping(col1) AS g, g + 1 AS g_plus
FROM gidSrc
GROUP BY col1 WITH ROLLUP
ORDER BY col1;

SELECT col1, grouping_id() AS gid, CASE WHEN gid = 0 THEN 'detail' ELSE 'total' END AS level
FROM gidSrc
GROUP BY col1 WITH ROLLUP
ORDER BY col1;

SELECT grouping_id() AS gid, SUM(col2) AS s, gid + s AS combined
FROM gidSrc
GROUP BY col1 WITH ROLLUP
ORDER BY col1;


-- Wide (34-column) grouping set
SELECT count(*)
FROM wide34
GROUP BY GROUPING SETS ((c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,
                         c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34));


-- Ordinal references inside ROLLUP / GROUPING SETS
SELECT col1, col2, SUM(col3)
FROM triStr
GROUP BY ROLLUP(1, 2)
ORDER BY col1 NULLS LAST, col2 NULLS LAST;

SELECT a, b, count(1)
FROM pairAb
GROUP BY GROUPING SETS ((1, a), (b))
ORDER BY a, b;


-- Aggregate functions in HAVING over grouping analytics (filtering rolled-up groups)
SELECT a, b, SUM(b) AS sb
FROM aggSrc
GROUP BY GROUPING SETS ((a, b), (a))
HAVING SUM(b) > 15
ORDER BY a, b;

SELECT a, b, SUM(b) AS sb
FROM aggSrc
GROUP BY CUBE(a, b)
HAVING SUM(b) > 15
ORDER BY a, b;

SELECT a, b, SUM(b) AS sb
FROM aggSrc
GROUP BY ROLLUP(a, b)
HAVING SUM(b) > 15
ORDER BY a, b;

SELECT a, b, SUM(b) AS sb
FROM aggSrc
GROUP BY GROUPING SETS ((a, b), (a))
HAVING COUNT(b) > 1
ORDER BY a, b;

SELECT a + 1 AS ak, b, SUM(b) AS sb
FROM aggSrc
GROUP BY GROUPING SETS ((a + 1, b), (a + 1))
HAVING SUM(b) > 15
ORDER BY ak, b;

SELECT a, b, SUM(a + b) AS s
FROM aggSrc
GROUP BY GROUPING SETS ((a, b), (a))
HAVING SUM(a + b) > 20
ORDER BY a, b;

SELECT c1
FROM qualSrc
GROUP BY ROLLUP(qualSrc.c1)
HAVING qualSrc.c1 = 1;


-- Aggregate functions in ORDER BY over grouping analytics
SELECT a, b, SUM(b) AS sb
FROM aggSrc
GROUP BY GROUPING SETS ((a, b), (a))
ORDER BY SUM(b);

SELECT a, b, SUM(b) AS sb
FROM aggSrc
GROUP BY CUBE(a, b)
ORDER BY SUM(b);

SELECT a, b, SUM(b) AS sb
FROM aggSrc
GROUP BY ROLLUP(a, b)
ORDER BY SUM(b);


-- Struct field access inside aggregates over grouping analytics
SELECT col1, col2, SUM(s1.val) AS sum1, SUM(s2.val) AS sum2, SUM(s3.val) AS sum3
FROM structThree
GROUP BY GROUPING SETS ((col1, col2), ())
ORDER BY col1;

SELECT col1, col2, SUM(s1.val) AS sum1
FROM structOne
GROUP BY ROLLUP (col1, col2)
ORDER BY col1, col2;

SELECT col1, SUM(s1.inner.val) AS sum1
FROM structNested
GROUP BY GROUPING SETS ((col1), ())
ORDER BY col1;


-- Uncorrelated scalar subquery in the SELECT list, combined with grouping analytics
SELECT col1, SUM(col2) AS s,
       (SELECT COUNT(*) FROM nums3) AS const_cnt
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

SELECT col1, SUM(col2) AS s,
       (SELECT COUNT(*) FROM nums3) AS const_cnt
FROM subOuter3
GROUP BY GROUPING SETS ((col1), ())
ORDER BY col1 NULLS LAST;

SELECT col1, col2, SUM(col3) AS s,
       (SELECT COUNT(*) FROM sub12) AS const_cnt
FROM cubeStr
GROUP BY CUBE(col1, col2)
ORDER BY col1 NULLS LAST, col2 NULLS LAST;

-- Multiple uncorrelated scalar subqueries with ROLLUP
SELECT col1, SUM(col2) AS s,
       (SELECT COUNT(*) FROM sub12) AS cnt,
       (SELECT MAX(y) FROM maxW) AS max_val
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Uncorrelated scalar subquery computing the same local expression as the ROLLUP grouping key
SELECT 1 + 1 AS gkey, SUM(col2) AS s,
       (SELECT 1 + 1 FROM zeroDummy) AS local_expr
FROM localOuter
GROUP BY ROLLUP(1 + 1)
ORDER BY gkey NULLS LAST;


-- Subquery in WHERE (pre-aggregation filter) with grouping analytics
SELECT col1, SUM(col2) AS s
FROM subOuter4
WHERE col1 IN (SELECT x FROM sub12)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

SELECT col1, SUM(col2) AS s
FROM subOuter4
WHERE EXISTS (SELECT 1 FROM sub12 WHERE sub12.x = subOuter4.col1)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

SELECT col1, SUM(col2) AS s
FROM notInOuter
WHERE col1 NOT IN (SELECT x FROM one3)
GROUP BY GROUPING SETS((col1), ())
ORDER BY col1 NULLS LAST;


-- Correlated subquery in the SELECT list whose predicate references a grouping key:
-- currently rejected when combined with ROLLUP (the correlated grouping key is treated
-- as a non-aggregating expression). Captured to lock down the current behavior.
SELECT col1, SUM(col2) AS s,
       EXISTS (SELECT 1 FROM sub12 WHERE sub12.x = col1) AS has_match
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Uncorrelated IN subquery in the SELECT list whose left side is a grouping key.
-- For the rolled-up grand-total row the grouping key is NULL.
SELECT col1, SUM(col2) AS s,
       col1 IN (SELECT x FROM sub13) AS in_result
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

SELECT col1 + col2 AS gkey, SUM(col3) AS s,
       (col1 + col2) IN (SELECT x FROM sub1132) AS in_result
FROM exprOuter
GROUP BY ROLLUP(col1 + col2)
ORDER BY gkey NULLS LAST;


-- Negative: grouping()/grouping_id() referencing a column that is not a grouping column
SELECT GROUPING(col2)
FROM gidSrc
GROUP BY ROLLUP(col1);

SELECT GROUPING_ID(col1, col2)
FROM gidSrc
GROUP BY ROLLUP(col1);

-- Negative: window function in GROUP BY
SELECT SUM(a)
FROM (SELECT 1 AS a) t
GROUP BY ROW_NUMBER() OVER (ORDER BY a);


-- DISTINCT aggregates, aggregate FILTER, and grouping-function-plus-aggregate
-- combinations over grouping analytics.

CREATE TEMPORARY VIEW distSrc AS SELECT * FROM VALUES
  (1, 10), (1, 10), (1, 20), (2, 30), (2, 30)
  AS distSrc(a, b);

-- DISTINCT aggregates over grouping analytics
SELECT a, COUNT(DISTINCT b) AS dcnt, SUM(DISTINCT b) AS dsum
FROM distSrc
GROUP BY ROLLUP(a)
ORDER BY a NULLS LAST;

SELECT a, COUNT(DISTINCT b) AS dcnt
FROM distSrc
GROUP BY CUBE(a)
ORDER BY a NULLS LAST;

-- DISTINCT aggregate in HAVING over grouping analytics
SELECT a, SUM(b) AS s
FROM distSrc
GROUP BY ROLLUP(a)
HAVING COUNT(DISTINCT b) > 1
ORDER BY a NULLS LAST;

-- Aggregate FILTER (WHERE ...) over grouping analytics (the FILTER predicate references
-- the original column, not the Expand-nullified grouping key)
SELECT a, b, SUM(b) FILTER (WHERE b > 15) AS sfilt
FROM aggSrc
GROUP BY CUBE(a, b)
ORDER BY a NULLS LAST, b NULLS LAST;

SELECT a, COUNT(*) FILTER (WHERE b > 15) AS cfilt
FROM aggSrc
GROUP BY ROLLUP(a)
ORDER BY a NULLS LAST;

-- Grouping function combined with an aggregate filter in HAVING
SELECT a, b, SUM(b) AS s
FROM aggSrc
GROUP BY CUBE(a, b)
HAVING GROUPING_ID(a, b) = 0 AND SUM(b) > 15
ORDER BY a, b;

SELECT a, SUM(b) AS s, GROUPING(a) AS ga
FROM aggSrc
GROUP BY ROLLUP(a)
HAVING SUM(b) > 30
ORDER BY a NULLS LAST;

-- GROUPING SETS with only the empty grouping set (grand total)
SELECT SUM(b) AS s
FROM aggSrc
GROUP BY GROUPING SETS (());


-- Multiple, nested, and otherwise complex subqueries combined with grouping analytics.

-- Nested scalar subquery (subquery inside a subquery) in the SELECT list
SELECT col1, SUM(col2) AS s,
       (SELECT COUNT(*) FROM nums3 WHERE x < (SELECT MAX(y) FROM maxW)) AS nested_cnt
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Scalar subquery whose inner query itself uses grouping analytics
SELECT col1, SUM(col2) AS s,
       (SELECT COUNT(*) FROM (SELECT a FROM aggSrc GROUP BY CUBE(a))) AS cube_rows
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Scalar subquery with its own GROUP BY inside, in the SELECT list
SELECT col1, SUM(col2) AS s,
       (SELECT COUNT(*) FROM (SELECT a FROM aggSrc GROUP BY a)) AS distinct_a
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- IN subquery in WHERE whose inner query uses grouping analytics
SELECT col1, col2, COUNT(*) AS cnt
FROM cubeStr
WHERE col1 IN (SELECT a FROM aggSrc GROUP BY ROLLUP(a) HAVING a IS NOT NULL)
GROUP BY CUBE(col1, col2)
ORDER BY col1 NULLS LAST, col2 NULLS LAST;

-- Nested IN subquery (IN inside IN) in WHERE
SELECT col1, SUM(col2) AS s
FROM subOuter4
WHERE col1 IN (SELECT x FROM sub13 WHERE x IN (SELECT a FROM aggSrc))
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Correlated EXISTS (pre-aggregation, on a base column) with a nested IN inside
SELECT col1, SUM(col2) AS s
FROM subOuter4
WHERE EXISTS (
  SELECT 1 FROM sub12
  WHERE sub12.x = subOuter4.col1 AND sub12.x IN (SELECT a FROM aggSrc)
)
GROUP BY GROUPING SETS((col1), ())
ORDER BY col1 NULLS LAST;

-- Uncorrelated scalar subquery in HAVING
SELECT a, SUM(b) AS s
FROM aggSrc
GROUP BY ROLLUP(a)
HAVING SUM(b) > (SELECT AVG(y) FROM maxW)
ORDER BY a NULLS LAST;

-- Two uncorrelated scalar subqueries combined in HAVING
SELECT a, SUM(b) AS s
FROM aggSrc
GROUP BY ROLLUP(a)
HAVING SUM(b) > (SELECT MIN(y) FROM maxW) AND COUNT(*) >= (SELECT COUNT(*) FROM sub12) - 2
ORDER BY a NULLS LAST;

-- Scalar subquery used inside an ORDER BY expression
SELECT a, SUM(b) AS s
FROM aggSrc
GROUP BY CUBE(a)
ORDER BY SUM(b) + (SELECT MAX(y) FROM maxW), a NULLS LAST;

-- Scalar subquery value combined arithmetically with an aggregate
SELECT a, b, SUM(b) + (SELECT MAX(y) FROM maxW) AS s_plus
FROM aggSrc
GROUP BY CUBE(a, b)
ORDER BY a NULLS LAST, b NULLS LAST;

-- Subqueries in three different clauses (SELECT, WHERE, HAVING) at once
SELECT col1, SUM(col2) AS s,
       (SELECT COUNT(*) FROM nums3) AS n
FROM subOuter4
WHERE col1 IN (SELECT x FROM sub12)
GROUP BY ROLLUP(col1)
HAVING SUM(col2) > (SELECT MIN(y) FROM maxW)
ORDER BY col1 NULLS LAST;

-- IN subquery in WHERE plus an uncorrelated scalar subquery in the SELECT list
SELECT col1, col2, COUNT(*) AS cnt, (SELECT SUM(y) FROM maxW) AS total
FROM cubeStr
WHERE col1 IN (SELECT a FROM aggSrc)
GROUP BY CUBE(col1, col2)
ORDER BY col1 NULLS LAST, col2 NULLS LAST;

-- NOT IN over a filtered subquery, with GROUPING SETS
SELECT col1, SUM(col2) AS s
FROM subOuter4
WHERE col1 NOT IN (SELECT x FROM sub12 WHERE x > 1)
GROUP BY GROUPING SETS((col1), ())
ORDER BY col1 NULLS LAST;

-- Uncorrelated IN in the SELECT list where the subquery aggregates
SELECT col1, SUM(col2) AS s,
       col1 IN (SELECT MAX(a) FROM aggSrc) AS is_max_a
FROM subOuter3
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;

-- Correlated scalar subquery in WHERE (pre-aggregation, on a base column)
SELECT col1, SUM(col2) AS s
FROM subOuter4
WHERE col2 > (SELECT AVG(x) FROM sub12 WHERE x <= subOuter4.col1)
GROUP BY ROLLUP(col1)
ORDER BY col1 NULLS LAST;


-- Clean up temporary views
DROP VIEW testData;
DROP VIEW courseSales;
DROP VIEW gidSrc;
DROP VIEW aggSrc;
DROP VIEW pairAb;
DROP VIEW qualSrc;
DROP VIEW triStr;
DROP VIEW subOuter3;
DROP VIEW subOuter4;
DROP VIEW notInOuter;
DROP VIEW exprOuter;
DROP VIEW cubeStr;
DROP VIEW localOuter;
DROP VIEW sub12;
DROP VIEW sub13;
DROP VIEW sub1132;
DROP VIEW one3;
DROP VIEW maxW;
DROP VIEW zeroDummy;
DROP VIEW nums3;
DROP VIEW wide34;
DROP VIEW structThree;
DROP VIEW structOne;
DROP VIEW structNested;
DROP VIEW distSrc;
