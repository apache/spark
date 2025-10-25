-- Tests covering column resolution priority in Aggregate.

CREATE TEMPORARY VIEW v1 AS VALUES (1, 1, 1), (2, 2, 1) AS t(a, b, k);
CREATE TEMPORARY VIEW v2 AS VALUES (1, 1, 1), (2, 2, 1) AS t(x, y, all);

-- Relation output columns have higher priority than lateral column alias. This query
-- should fail as `b` is not in GROUP BY.
SELECT max(a) AS b, b FROM v1 GROUP BY k;

-- Lateral column alias has higher priority than outer reference.
SELECT a FROM v1 WHERE (12, 13) IN (SELECT max(x + 10) AS a, a + 1 FROM v2);

-- Relation output columns have higher priority than GROUP BY alias. This query should
-- fail as `a` is not in GROUP BY.
SELECT a AS k FROM v1 GROUP BY k;

-- Relation output columns have higher priority than GROUP BY ALL. This query should
-- fail as `x` is not in GROUP BY.
SELECT x FROM v2 GROUP BY all;

-- GROUP BY alias has higher priority than GROUP BY ALL, this query fails as `b` is not in GROUP BY.
SELECT a AS all, b FROM v1 GROUP BY all;

-- GROUP BY alias/ALL does not support lateral column alias.
SELECT k AS lca, lca + 1 AS col FROM v1 GROUP BY k, col;
SELECT k AS lca, lca + 1 AS col FROM v1 GROUP BY all;

-- GROUP BY alias still works if it does not directly reference lateral column alias.
SELECT k AS lca, lca + 1 AS col FROM v1 GROUP BY lca;

-- GROUP BY ALL has higher priority than outer reference. This query should run as `a` and `b` are
-- in GROUP BY due to the GROUP BY ALL resolution.
SELECT * FROM v2 WHERE EXISTS (SELECT a, b FROM v1 GROUP BY all);

-- Remove unnecessary Aliases from grouping and aggregate expressions
CREATE TEMPORARY VIEW vcol(col2 STRING) AS VALUES ('{"str": "test"}');
SELECT timestamp(col2:str) FROM vcol GROUP BY timestamp(col2:str);

-- Correctly fallback from ambiguous reference due to ExtractValue alias
CREATE TEMPORARY VIEW vcol2(col1 STRUCT<a: STRING>, a STRING) AS VALUES (named_struct('a', 'nested'), 'outer');
SELECT col1.a, a FROM vcol2 ORDER BY a;
SELECT col1.a, a FROM vcol2 ORDER BY col1.a;

-- Don't collapse aliases before alias name is constructed
SELECT DISTINCT(col1.a:b.c AS out1, col1.a:b.d AS out2) FROM vcol2;

-- Deduplicate expressions before adding them to Aggregate
CREATE TEMPORARY VIEW vcol3(col1 INT, col2 INT) AS VALUES (1, 2), (3, 4);
SELECT col1 FROM vcol3 GROUP BY col1 HAVING MAX(col2) == (SELECT 1 WHERE MAX(col2) = 1);
SELECT col1 FROM vcol3 GROUP BY col1 HAVING (SELECT 1 WHERE MAX(col2) = 1) == MAX(col2);
SELECT col1 FROM vcol3 GROUP BY col1 HAVING (SELECT 1 WHERE MAX(col2) = 1) == (SELECT 1 WHERE MAX(col2) = 1);
SELECT col1 FROM vcol3 GROUP BY col1 HAVING bool_or(col2 = 1) AND bool_or(col2 = 1);
SELECT 1 GROUP BY COALESCE(1, 1) HAVING COALESCE(1, 1) = 1  OR COALESCE(1, 1) IS NOT NULL;
SELECT col1 FROM vcol3 t1 GROUP BY col1 HAVING (
    SELECT MAX(t2.col1) FROM vcol3 t2 WHERE t2.col1 == MAX(t1.col1) GROUP BY t2.col1 HAVING (
        SELECT t3.col1 FROM vcol3 t3 WHERE t3.col1 == MAX(t2.col1)
    )
);

DROP VIEW vcol;
DROP VIEW vcol2;
DROP VIEW vcol3;
