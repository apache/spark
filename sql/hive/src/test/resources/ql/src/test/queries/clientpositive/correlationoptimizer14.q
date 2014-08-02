set hive.optimize.reducededuplication=true;
set hive.optimize.reducededuplication.min.reducer=1;
set hive.optimize.correlation=true;
-- This file is used to show plans of queries involving cluster by, distribute by,
-- order by, and sort by.
-- Right now, Correlation optimizer check the most restrictive condition
-- when determining if a ReduceSinkOperator is not necessary.
-- This condition is that two ReduceSinkOperators should have same sorting columns,
-- same partitioning columns, same sorting orders and no conflict on the numbers of reducers. 

-- Distribute by will not be optimized because distribute by does not introduce
-- sorting columns.
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x DISTRIBUTE BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y DISTRIBUTE BY key) yy
ON (xx.key=yy.key);

-- Sort by will not be optimized because sort by does not introduce partitioning columns
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x SORT BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y SORT BY key) yy
ON (xx.key=yy.key);

set hive.optimize.correlation=false;
-- Distribute by and sort by on the same key(s) should be optimized
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x DISTRIBUTE BY key SORT BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y DISTRIBUTE BY key SORT BY key) yy
ON (xx.key=yy.key);

SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x DISTRIBUTE BY key SORT BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y DISTRIBUTE BY key SORT BY key) yy
ON (xx.key=yy.key);

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x DISTRIBUTE BY key SORT BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y DISTRIBUTE BY key SORT BY key) yy
ON (xx.key=yy.key);

SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x DISTRIBUTE BY key SORT BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y DISTRIBUTE BY key SORT BY key) yy
ON (xx.key=yy.key);

set hive.optimize.correlation=true;
-- Because for join we use ascending order, if sort by uses descending order,
-- this query will not be optimized
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x DISTRIBUTE BY key SORT BY key DESC) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y DISTRIBUTE BY key SORT BY key DESC) yy
ON (xx.key=yy.key);

-- Even if hive.optimize.reducededuplication.min.reducer=1, order by will not be optimized
-- because order by does not introduce partitioning columns
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x ORDER BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y ORDER BY key) yy
ON (xx.key=yy.key);

set hive.optimize.correlation=false;
-- Cluster by will be optimized
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x Cluster BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y Cluster BY key) yy
ON (xx.key=yy.key);

SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x Cluster BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y Cluster BY key) yy
ON (xx.key=yy.key);

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x Cluster BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y Cluster BY key) yy
ON (xx.key=yy.key);

SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x Cluster BY key) xx
JOIN
(SELECT y.key as key, y.value as value FROM src1 y Cluster BY key) yy
ON (xx.key=yy.key);

set hive.optimize.correlation=false;
-- If hive.optimize.reducededuplication.min.reducer=1,
-- group by and then order by should be optimized
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x CLUSTER BY key) xx
JOIN
(SELECT y.key as key, count(*) as value FROM src1 y GROUP BY y.key ORDER BY key) yy
ON (xx.key=yy.key);

SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x CLUSTER BY key) xx
JOIN
(SELECT y.key as key, count(*) as value FROM src1 y GROUP BY y.key ORDER BY key) yy
ON (xx.key=yy.key);

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x CLUSTER BY key) xx
JOIN
(SELECT y.key as key, count(*) as value FROM src1 y GROUP BY y.key ORDER BY key) yy
ON (xx.key=yy.key);

SELECT xx.key, xx.value, yy.key, yy.value
FROM
(SELECT x.key as key, x.value as value FROM src x CLUSTER BY key) xx
JOIN
(SELECT y.key as key, count(*) as value FROM src1 y GROUP BY y.key ORDER BY key) yy
ON (xx.key=yy.key);
