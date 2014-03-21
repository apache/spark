set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 6 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery xx, subquery yy, and xx join yy.
EXPLAIN
SELECT xx.key, xx.cnt, yy.key, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.cnt;

SELECT xx.key, xx.cnt, yy.key, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.cnt, yy.key, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.cnt;

SELECT xx.key, xx.cnt, yy.key, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.cnt;

set hive.optimize.correlation=true;
set hive.auto.convert.join=true;
-- Enable hive.auto.convert.join.
EXPLAIN
SELECT xx.key, xx.cnt, yy.key, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.cnt;

SELECT xx.key, xx.cnt, yy.key, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.cnt;

set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 3 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery yy and xx join yy.
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x GROUP BY x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x GROUP BY x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x GROUP BY x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x GROUP BY x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery yy and xx join yy.
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key ORDER BY xx.key, yy.key, yy.cnt;

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

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery xx and xx join yy join zz.
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN src zz ON xx.key=zz.key
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON zz.key=yy.key 
ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN src zz ON xx.key=zz.key
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON zz.key=yy.key 
ORDER BY xx.key, yy.key, yy.cnt;

set hive.optimize.correlation=true;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery yy and xx join yy join zz.
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN src zz ON xx.key=zz.key
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON zz.key=yy.key 
ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN src zz ON xx.key=zz.key
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON zz.key=yy.key 
ORDER BY xx.key, yy.key, yy.cnt;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery yy and xx join yy join zz.
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key JOIN src zz
ON yy.key=zz.key ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key JOIN src zz
ON yy.key=zz.key ORDER BY xx.key, yy.key, yy.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key JOIN src zz
ON yy.key=zz.key ORDER BY xx.key, yy.key, yy.cnt;

SELECT xx.key, yy.key, yy.cnt
FROM src1 xx
JOIN
(SELECT x.key as key, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key) yy
ON xx.key=yy.key JOIN src zz
ON yy.key=zz.key ORDER BY xx.key, yy.key, yy.cnt;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 6 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery tmp and tmp join z.
EXPLAIN
SELECT tmp.key, tmp.sum1, tmp.sum2, z.key, z.value
FROM
(SELECT xx.key as key, sum(xx.cnt) as sum1, sum(yy.cnt) as sum2
 FROM (SELECT x.key as key, count(*) AS cnt FROM src x group by x.key) xx
 JOIN (SELECT y.key as key, count(*) AS cnt FROM src1 y group by y.key) yy
 ON (xx.key=yy.key) GROUP BY xx.key) tmp
JOIN src z ON tmp.key=z.key
ORDER BY tmp.key, tmp.sum1, tmp.sum2, z.key, z.value;

SELECT tmp.key, tmp.sum1, tmp.sum2, z.key, z.value
FROM
(SELECT xx.key as key, sum(xx.cnt) as sum1, sum(yy.cnt) as sum2
 FROM (SELECT x.key as key, count(*) AS cnt FROM src x group by x.key) xx
 JOIN (SELECT y.key as key, count(*) AS cnt FROM src1 y group by y.key) yy
 ON (xx.key=yy.key) GROUP BY xx.key) tmp
JOIN src z ON tmp.key=z.key
ORDER BY tmp.key, tmp.sum1, tmp.sum2, z.key, z.value;

set hive.optimize.correlation=true;
EXPLAIN
SELECT tmp.key, tmp.sum1, tmp.sum2, z.key, z.value
FROM
(SELECT xx.key as key, sum(xx.cnt) as sum1, sum(yy.cnt) as sum2
 FROM (SELECT x.key as key, count(*) AS cnt FROM src x group by x.key) xx
 JOIN (SELECT y.key as key, count(*) AS cnt FROM src1 y group by y.key) yy
 ON (xx.key=yy.key) GROUP BY xx.key) tmp
JOIN src z ON tmp.key=z.key
ORDER BY tmp.key, tmp.sum1, tmp.sum2, z.key, z.value;

SELECT tmp.key, tmp.sum1, tmp.sum2, z.key, z.value
FROM
(SELECT xx.key as key, sum(xx.cnt) as sum1, sum(yy.cnt) as sum2
 FROM (SELECT x.key as key, count(*) AS cnt FROM src x group by x.key) xx
 JOIN (SELECT y.key as key, count(*) AS cnt FROM src1 y group by y.key) yy
 ON (xx.key=yy.key) GROUP BY xx.key) tmp
JOIN src z ON tmp.key=z.key
ORDER BY tmp.key, tmp.sum1, tmp.sum2, z.key, z.value;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 6 MR jobs are needed.
-- When Correlation Optimizer is turned on, 4 MR jobs are needed.
-- 2 MR jobs are used to evaluate yy, 1 MR is used to evaluate xx and xx join yy.
-- The last MR is used for ordering. 
EXPLAIN
SELECT xx.key, xx.cnt, yy.key, yy.value, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, x.value as value, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key, x.value) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.value, yy.cnt;

SELECT xx.key, xx.cnt, yy.key, yy.value, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, x.value as value, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key, x.value) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.value, yy.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.cnt, yy.key, yy.value, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, x.value as value, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key, x.value) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.value, yy.cnt;

SELECT xx.key, xx.cnt, yy.key, yy.value, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, x.value as value, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key, x.value) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.value, yy.cnt;

set hive.optimize.correlation=true;
set hive.auto.convert.join=true;
EXPLAIN
SELECT xx.key, xx.cnt, yy.key, yy.value, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, x.value as value, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key, x.value) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.value, yy.cnt;

SELECT xx.key, xx.cnt, yy.key, yy.value, yy.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
JOIN
(SELECT x.key as key, x.value as value, count(1) as cnt FROM src x JOIN src y ON (x.key = y.key) group by x.key, x.value) yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt, yy.key, yy.value, yy.cnt;

