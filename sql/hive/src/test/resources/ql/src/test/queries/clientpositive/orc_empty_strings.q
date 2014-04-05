CREATE TABLE test_orc (key STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

INSERT OVERWRITE TABLE test_orc SELECT '' FROM src limit 10;

-- Test reading a column which is just empty strings

SELECT * FROM test_orc; 

INSERT OVERWRITE TABLE test_orc SELECT IF (key % 3 = 0, key, '') FROM src limit 10;

-- Test reading a column which has some empty strings

SELECT * FROM test_orc ORDER BY key;
