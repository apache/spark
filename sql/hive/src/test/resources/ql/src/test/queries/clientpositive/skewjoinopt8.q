set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key) ON ((3), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

CREATE TABLE T3(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3;

-- This test is for validating skewed join compile time optimization for more than
-- 2 tables. The join key is the same, and so a 3-way join would be performed.
-- 1 of the 3 tables are skewed on the join key
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.*, c.* FROM T1 a JOIN T2 b ON a.key = b.key JOIN T3 c on a.key = c.key;

SELECT a.*, b.*, c.* FROM T1 a JOIN T2 b ON a.key = b.key JOIN T3 c on a.key = c.key
ORDER BY a.key, b.key, c.key, a.val, b.val, c.val;
