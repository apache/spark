set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key) ON ((2), (7)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key) ON ((3), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

-- a simple query with skew on both the tables on the join key
-- multiple skew values are present for the skewed keys
-- but the skewed values do not overlap.
-- The join values are a superset of the skewed keys.
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val;

SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1 a LEFT OUTER JOIN T2 b ON a.key = b.key and a.val = b.val;

SELECT a.*, b.* FROM T1 a LEFT OUTER JOIN T2 b ON a.key = b.key and a.val = b.val
ORDER BY a.key, b.key, a.val, b.val;

-- a group by at the end should not change anything

EXPLAIN
SELECT a.key, count(1) FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val group by a.key;

SELECT a.key, count(1) FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val group by a.key;

EXPLAIN
SELECT a.key, count(1) FROM T1 a LEFT OUTER JOIN T2 b ON a.key = b.key and a.val = b.val group by a.key;

SELECT a.key, count(1) FROM T1 a LEFT OUTER JOIN T2 b ON a.key = b.key and a.val = b.val group by a.key;
