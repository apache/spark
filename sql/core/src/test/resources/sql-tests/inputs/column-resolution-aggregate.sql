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
