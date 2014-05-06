set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set mapred.reduce.tasks = 2;

-- Tests that when a multi insert inserts into a bucketed table and a table which is not bucketed
-- the bucketed table is not merged and the table which is not bucketed is

CREATE TABLE bucketed_table(key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE unbucketed_table(key INT, value STRING);

EXPLAIN EXTENDED
FROM src
INSERT OVERWRITE TABLE bucketed_table SELECT key, value
INSERT OVERWRITE TABLE unbucketed_table SELECT key, value cluster by key;

FROM src
INSERT OVERWRITE TABLE bucketed_table SELECT key, value
INSERT OVERWRITE TABLE unbucketed_table SELECT key, value cluster by key;

DESC FORMATTED bucketed_table;

SELECT * FROM bucketed_table TABLESAMPLE (BUCKET 1 OUT OF 2) s LIMIT 10;
SELECT * FROM bucketed_table TABLESAMPLE (BUCKET 2 OUT OF 2) s LIMIT 10;

-- Should be 2 (not merged)
SELECT COUNT(DISTINCT INPUT__FILE__NAME) FROM bucketed_table;

-- Should be 1 (merged)
SELECT COUNT(DISTINCT INPUT__FILE__NAME) FROM unbucketed_table;
