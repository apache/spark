set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(key STRING, val STRING)
CLUSTERED BY (key) INTO 4 BUCKETS
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

-- add a test where the skewed key is also the bucketized key
-- it should not matter, and the compile time skewed join
-- optimization is performed
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key;

SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
