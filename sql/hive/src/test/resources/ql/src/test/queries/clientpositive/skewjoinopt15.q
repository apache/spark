set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE tmpT1(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE tmpT1;

-- testing skew on other data types - int
CREATE TABLE T1(key INT, val STRING) SKEWED BY (key) ON ((2));
INSERT OVERWRITE TABLE T1 SELECT key, val FROM tmpT1;

CREATE TABLE tmpT2(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE tmpT2;

CREATE TABLE T2(key INT, val STRING) SKEWED BY (key) ON ((3));

INSERT OVERWRITE TABLE T2 SELECT key, val FROM tmpT2;

-- The skewed key is a integer column.
-- Otherwise this test is similar to skewjoinopt1.q
-- Both the joined tables are skewed, and the joined column
-- is an integer
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key;

SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key;

SELECT a.*, b.* FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- an aggregation at the end should not change anything

EXPLAIN
SELECT count(1) FROM T1 a JOIN T2 b ON a.key = b.key;

SELECT count(1) FROM T1 a JOIN T2 b ON a.key = b.key;

EXPLAIN
SELECT count(1) FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key;

SELECT count(1) FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key;
