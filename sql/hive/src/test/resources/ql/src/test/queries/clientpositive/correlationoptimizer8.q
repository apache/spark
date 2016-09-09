set hive.auto.convert.join=false;
set hive.optimize.correlation=false;
-- When the Correlation Optimizer is turned off, this query will be evaluated by
-- 4 MR jobs. 
-- When the Correlation Optimizer is turned on, because both inputs of the 
-- UnionOperator are correlated, we can use 2 MR jobs to evaluate this query.
-- The first MR job will evaluate subquery subq1 and subq1 join x. The second
-- MR is for ordering. 
EXPLAIN
SELECT x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key) ORDER BY x.key, x.value, subq1.cnt;

SELECT x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key) ORDER BY x.key, x.value, subq1.cnt;

set hive.optimize.correlation=true;
EXPLAIN
SELECT x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key) ORDER BY x.key, x.value, subq1.cnt;

SELECT x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key) ORDER BY x.key, x.value, subq1.cnt;

set hive.optimize.correlation=false;
-- When the Correlation Optimizer is turned off, this query will be evaluated by
-- 4 MR jobs. 
-- When the Correlation Optimizer is turned on, because both inputs of the 
-- UnionOperator are correlated, we can use 2 MR jobs to evaluate this query.
-- The first MR job will evaluate subquery subq1 and subq1 join x. The second
-- MR is for ordering. 
EXPLAIN
SELECT subq1.key, subq1.cnt, x.key, x.value
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.value as key, count(1) as cnt from src1 x1 where x1.key > 100 group by x1.value
) subq1
LEFT OUTER JOIN src1 x ON (x.key = subq1.key) ORDER BY subq1.key, subq1.cnt, x.key, x.value;

SELECT subq1.key, subq1.cnt, x.key, x.value
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.value as key, count(1) as cnt from src1 x1 where x1.key > 100 group by x1.value
) subq1
LEFT OUTER JOIN src1 x ON (x.key = subq1.key) ORDER BY subq1.key, subq1.cnt, x.key, x.value;

set hive.optimize.correlation=true;
EXPLAIN
SELECT subq1.key, subq1.cnt, x.key, x.value
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.value as key, count(1) as cnt from src1 x1 where x1.key > 100 group by x1.value
) subq1
LEFT OUTER JOIN src1 x ON (x.key = subq1.key) ORDER BY subq1.key, subq1.cnt, x.key, x.value;

SELECT subq1.key, subq1.cnt, x.key, x.value
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.value as key, count(1) as cnt from src1 x1 where x1.key > 100 group by x1.value
) subq1
LEFT OUTER JOIN src1 x ON (x.key = subq1.key) ORDER BY subq1.key, subq1.cnt, x.key, x.value;

set hive.optimize.correlation=true;
-- When the Correlation Optimizer is turned on, because a input of UnionOperator is
-- not correlated, we cannot handle this case right now. So, this query will not be
-- optimized. 
EXPLAIN
SELECT x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key, x1.value
) subq1
JOIN src1 x ON (x.key = subq1.key) ORDER BY x.key, x.value, subq1.cnt;

set hive.optimize.correlation=true;
-- When the Correlation Optimizer is turned on, because a input of UnionOperator is
-- not correlated, we cannot handle this case right now. So, this query will not be
-- optimized. 
EXPLAIN
SELECT subq1.key, subq1.value, x.key, x.value
FROM 
( SELECT cast(x.key as INT) as key, count(1) as value from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT count(1) as key, cast(x1.key as INT) as value from src x1 where x1.key > 100 group by x1.key
) subq1
FULL OUTER JOIN src1 x ON (x.key = subq1.key) ORDER BY subq1.key, subq1.value, x.key, x.value;
