--SET spark.sql.leafNodeDefaultParallelism=1
-- Tests covering column resolution priority in Sort.

CREATE TEMPORARY VIEW v1 AS VALUES (1, 2, 2), (2, 1, 1) AS t(a, b, k);
CREATE TEMPORARY VIEW v2 AS VALUES (1, 2, 2), (2, 1, 1) AS t(a, b, all);

-- Relation output columns have higher priority than missing reference.
-- Query will fail if we order by the column `v1.b`, as it's not in GROUP BY.
-- Actually results are [1, 2] as we order by `max(a) AS b`.
SELECT max(a) AS b FROM v1 GROUP BY k ORDER BY b;

-- Missing reference has higher priority than ORDER BY ALL.
-- Results will be [1, 2] if we order by `max(a)`.
-- Actually results are [2, 1] as we order by the grouping column `v2.all`.
SELECT max(a) FROM v2 GROUP BY all ORDER BY all;

-- ORDER BY ALL has higher priority than outer reference.
-- Results will be [1, 1] if we order by outer reference 'v2.all'.
-- Actually results are [2, 2] as we order by column `v1.b`
SELECT (SELECT b FROM v1 ORDER BY all LIMIT 1) FROM v2;
