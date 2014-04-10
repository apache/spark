set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=6000;

-- we will generate one MR job.
EXPLAIN
SELECT tmp.key
FROM (SELECT x1.key AS key FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      UNION ALL
      SELECT x2.key AS key FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)) tmp
ORDER BY tmp.key;

SELECT tmp.key
FROM (SELECT x1.key AS key FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      UNION ALL
      SELECT x2.key AS key FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)) tmp
ORDER BY tmp.key;

set hive.auto.convert.join.noconditionaltask.size=400;
-- Check if the total size of local tables will be
-- larger than the limit that
-- we set through hive.auto.convert.join.noconditionaltask.size (right now, it is
-- 400 bytes). If so, do not merge.
-- For this query, we will merge the MapJoin of x2 and y2 into the MR job
-- for UNION ALL and ORDER BY. But, the MapJoin of x1 and y2 will not be merged
-- into that MR job.
EXPLAIN
SELECT tmp.key
FROM (SELECT x1.key AS key FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      UNION ALL
      SELECT x2.key AS key FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)) tmp
ORDER BY tmp.key;

SELECT tmp.key
FROM (SELECT x1.key AS key FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      UNION ALL
      SELECT x2.key AS key FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)) tmp
ORDER BY tmp.key;

set hive.auto.convert.join.noconditionaltask.size=6000;
-- We will use two jobs.
-- We will generate one MR job for GROUP BY
-- on x1, one MR job for both the MapJoin of x2 and y2, the UNION ALL, and the
-- ORDER BY.
EXPLAIN
SELECT tmp.key
FROM (SELECT x1.key AS key FROM src1 x1 GROUP BY x1.key
      UNION ALL
      SELECT x2.key AS key FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)) tmp
ORDER BY tmp.key;

SELECT tmp.key
FROM (SELECT x1.key AS key FROM src1 x1 GROUP BY x1.key
      UNION ALL
      SELECT x2.key AS key FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)) tmp
ORDER BY tmp.key;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is disabled,
-- we will use 5 jobs.
-- We will generate one MR job to evaluate the sub-query tmp1,
-- one MR job to evaluate the sub-query tmp2,
-- one MR job for the Join of tmp1 and tmp2,
-- one MR job for aggregation on the result of the Join of tmp1 and tmp2,
-- and one MR job for the ORDER BY.
EXPLAIN
SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

set hive.optimize.correlation=true;
-- When Correlation Optimizer is enabled,
-- we will use two jobs. This first MR job will evaluate sub-queries of tmp1, tmp2,
-- the Join of tmp1 and tmp2, and the aggregation on the result of the Join of
-- tmp1 and tmp2. The second job will do the ORDER BY.
EXPLAIN
SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src x1 JOIN src1 y1 ON (x1.key = y1.key)
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

set hive.optimize.correlation=false;
-- When Correlation Optimizer is disabled,
-- we will use five jobs.
-- We will generate one MR job to evaluate the sub-query tmp1,
-- one MR job to evaluate the sub-query tmp2,
-- one MR job for the Join of tmp1 and tmp2,
-- one MR job for aggregation on the result of the Join of tmp1 and tmp2,
-- and one MR job for the ORDER BY.
EXPLAIN
SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src1 x1
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src1 x1
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

set hive.optimize.correlation=true;
-- When Correlation Optimizer is enabled,
-- we will use two job. This first MR job will evaluate sub-queries of tmp1, tmp2,
-- the Join of tmp1 and tmp2, and the aggregation on the result of the Join of
-- tmp1 and tmp2. The second job will do the ORDER BY.
EXPLAIN
SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src1 x1
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

SELECT tmp1.key as key, count(*) as cnt
FROM (SELECT x1.key AS key
      FROM src1 x1
      GROUP BY x1.key) tmp1
JOIN (SELECT x2.key AS key
      FROM src x2 JOIN src1 y2 ON (x2.key = y2.key)
      GROUP BY x2.key) tmp2
ON (tmp1.key = tmp2.key)
GROUP BY tmp1.key
ORDER BY key, cnt;

-- Check if we can correctly handle partitioned table.
CREATE TABLE part_table(key string, value string) PARTITIONED BY (partitionId int);
INSERT OVERWRITE TABLE part_table PARTITION (partitionId=1)
  SELECT key, value FROM src ORDER BY key, value LIMIT 100;
INSERT OVERWRITE TABLE part_table PARTITION (partitionId=2)
  SELECT key, value FROM src1 ORDER BY key, value;

EXPLAIN
SELECT count(*)
FROM part_table x JOIN src1 y ON (x.key = y.key);

SELECT count(*)
FROM part_table x JOIN src1 y ON (x.key = y.key);

