CREATE TABLE test_orc (key STRING)
PARTITIONED BY (part STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- Create a table with one column write to a partition, then add an additional column and write
-- to another partition
-- This can produce unexpected results with CombineHiveInputFormat

INSERT OVERWRITE TABLE test_orc PARTITION (part = '1') SELECT key FROM src tablesample (5 rows);

ALTER TABLE test_orc ADD COLUMNS (cnt INT);

INSERT OVERWRITE TABLE test_orc PARTITION (part = '2') SELECT key, count(*) FROM src GROUP BY key LIMIT 5;

SELECT * FROM test_orc ORDER BY key; 
