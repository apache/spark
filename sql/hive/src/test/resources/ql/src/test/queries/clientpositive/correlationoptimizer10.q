set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery xx and xx join yy.
-- This case is used to test LEFT SEMI JOIN since Hive will
-- introduce a GroupByOperator before the ReduceSinkOperator of
-- the right table (yy in queries below)
-- of LEFT SEMI JOIN.
EXPLAIN
SELECT xx.key, xx.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
LEFT SEMI JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt;

SELECT xx.key, xx.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
LEFT SEMI JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
LEFT SEMI JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt;

SELECT xx.key, xx.cnt
FROM
(SELECT x.key as key, count(1) as cnt FROM src1 x JOIN src1 y ON (x.key = y.key) group by x.key) xx
LEFT SEMI JOIN src yy
ON xx.key=yy.key ORDER BY xx.key, xx.cnt;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- The first job will evaluate subquery xx and xx join yy.
-- This case is used to test LEFT SEMI JOIN since Hive will
-- introduce a GroupByOperator before the ReduceSinkOperator of
-- the right table (yy in queries below)
-- of LEFT SEMI JOIN.
EXPLAIN
SELECT xx.key, xx.value
FROM
src1 xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND
       y.key > 20) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;

SELECT xx.key, xx.value
FROM
src1 xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND
       y.key > 20) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.value
FROM
src1 xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND
       y.key > 20) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;

SELECT xx.key, xx.value
FROM
src1 xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND
       y.key > 20) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is turned off, 4 MR jobs are needed.
-- When Correlation Optimizer is turned on, 2 MR jobs are needed.
-- This test is used to test if we can use shared scan for 
-- xx, yy:x, and yy:y.
EXPLAIN
SELECT xx.key, xx.value
FROM
src xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND x.key > 180) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;

SELECT xx.key, xx.value
FROM
src xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND x.key > 180) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;

set hive.optimize.correlation=true;
EXPLAIN
SELECT xx.key, xx.value
FROM
src xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND x.key > 180) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;

SELECT xx.key, xx.value
FROM
src xx
LEFT SEMI JOIN 
(SELECT x.key as key 
 FROM src x JOIN src y ON (x.key = y.key)
 WHERE x.key < 200 AND x.key > 180) yy
ON xx.key=yy.key ORDER BY xx.key, xx.value;
