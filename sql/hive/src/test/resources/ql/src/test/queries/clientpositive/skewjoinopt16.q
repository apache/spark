set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key, val) ON ((2, 12)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

-- One of the tables is skewed by 2 columns, and the other table is
-- skewed by one column. Ths join is performed on the both the columns
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val;

SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key and a.val = b.val
ORDER BY a.key, b.key, a.val, b.val;
