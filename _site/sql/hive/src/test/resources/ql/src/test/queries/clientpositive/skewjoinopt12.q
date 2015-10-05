set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key, val) ON ((2, 12), (8, 18)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key, val) ON ((3, 13), (8, 18)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

-- Both the join tables are skewed by 2 keys, and one of the skewed values
-- is common to both the tables. The join key matches the skewed key set.
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val;

SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val
ORDER BY a.key, b.key, a.val, b.val;
