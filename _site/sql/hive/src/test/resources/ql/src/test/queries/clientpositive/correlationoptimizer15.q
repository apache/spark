set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery xx and xx join yy.
EXPLAIN
SELECT xx.key, xx.cnt, yy.key
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key;

SELECT xx.key, xx.cnt, yy.key
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key;

set hive.optimize.correlation=true;
set hive.join.emit.interval=1;
EXPLAIN
SELECT xx.key, xx.cnt, yy.key
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key;

SELECT xx.key, xx.cnt, yy.key
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key;
