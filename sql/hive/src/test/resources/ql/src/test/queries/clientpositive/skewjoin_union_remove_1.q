set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;
set hive.mapred.supports.subdirectories=true;

set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.input.dir.recursive=true;

-- This is to test the union->selectstar->filesink and skewjoin optimization
-- Union of 2 map-reduce subqueries is performed for the skew join
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output, it might be easier to run the test
-- only on hadoop 23

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key) ON ((2)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key) ON ((3)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../data/files/T2.txt' INTO TABLE T2;

-- a simple join query with skew on both the tables on the join key

EXPLAIN
SELECT * FROM T1 a JOIN T2 b ON a.key = b.key;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT * FROM T1 a JOIN T2 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key;

SELECT a.*, b.* FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

create table DEST1(key1 STRING, val1 STRING, key2 STRING, val2 STRING);

EXPLAIN
INSERT OVERWRITE TABLE DEST1
SELECT * FROM T1 a JOIN T2 b ON a.key = b.key;

INSERT OVERWRITE TABLE DEST1
SELECT * FROM T1 a JOIN T2 b ON a.key = b.key;

SELECT * FROM DEST1
ORDER BY key1, key2, val1, val2;

EXPLAIN
INSERT OVERWRITE TABLE DEST1
SELECT * FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key;

INSERT OVERWRITE TABLE DEST1
SELECT * FROM T1 a RIGHT OUTER JOIN T2 b ON a.key = b.key;

SELECT * FROM DEST1
ORDER BY key1, key2, val1, val2;
