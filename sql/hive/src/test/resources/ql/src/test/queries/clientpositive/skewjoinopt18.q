set hive.mapred.supports.subdirectories=true;
set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE tmpT1(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE tmpT1;

-- testing skew on other data types - int
CREATE TABLE T1(key INT, val STRING) SKEWED BY (key) ON ((2));
INSERT OVERWRITE TABLE T1 SELECT key, val FROM tmpT1;

-- Tke skewed column is same in both the tables, however it is
-- INT in one of the tables, and STRING in the other table

CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T2.txt' INTO TABLE T2;

-- Once HIVE-3445 is fixed, the compile time skew join optimization would be
-- applicable here. Till the above jira is fixed, it would be performed as a
-- regular join
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key;

SELECT a.*, b.* FROM T1 a JOIN T2 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
