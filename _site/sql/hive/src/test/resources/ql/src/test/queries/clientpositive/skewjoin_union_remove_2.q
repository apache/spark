set hive.internal.ddl.list.bucketing.enable=true;
set hive.optimize.skewjoin.compiletime = true;
set hive.mapred.supports.subdirectories=true;

set hive.stats.autogather=false;
set hive.optimize.union.remove=true;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set mapred.input.dir.recursive=true;

CREATE TABLE T1(key STRING, val STRING)
SKEWED BY (key) ON ((2), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1;

CREATE TABLE T2(key STRING, val STRING)
SKEWED BY (key) ON ((3), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2;

CREATE TABLE T3(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T3.txt' INTO TABLE T3;

-- This is to test the union->selectstar->filesink and skewjoin optimization
-- Union of 3 map-reduce subqueries is performed for the skew join
-- There is no need to write the temporary results of the sub-queries, and then read them 
-- again to process the union. The union can be removed completely.
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)
-- Since this test creates sub-directories for the output table, it might be easier
-- to run the test only on hadoop 23

EXPLAIN
SELECT a.*, b.*, c.* FROM T1 a JOIN T2 b ON a.key = b.key JOIN T3 c on a.key = c.key;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

SELECT a.*, b.*, c.* FROM T1 a JOIN T2 b ON a.key = b.key JOIN T3 c on a.key = c.key
ORDER BY a.key, b.key, c.key, a.val, b.val, c.val;
