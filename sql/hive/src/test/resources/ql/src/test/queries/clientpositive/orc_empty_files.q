CREATE TABLE test_orc (key STRING, cnt INT)
CLUSTERED BY (key) INTO 3 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

set hive.enforce.bucketing=true;
set hive.exec.reducers.max = 1;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- Creates a table bucketed into 3 buckets, but only one contains data, specifically bucket 1,
-- buckets 0 and 2 are empty, so this tests reading from and empty file followed by a file
-- containing data and a file containing data followed by an empty file.
-- This can produce unexpected results with CombineHiveInputFormat

INSERT OVERWRITE TABLE test_orc SELECT one, COUNT(*) FROM (SELECT 1 AS one FROM src) a GROUP BY one;

SELECT count(*) FROM test_orc; 
