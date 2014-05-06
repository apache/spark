CREATE TABLE test_orc (key STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

ALTER TABLE test_orc SET SERDEPROPERTIES ('orc.row.index.stride' = '1000');

-- nulls.txt is a file containing a non-null string row followed by 1000 null string rows
-- this produces the effect that the number of non-null rows between the last and second
-- to last index stride are the same (there's only two index strides)

CREATE TABLE src_null(a STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../data/files/nulls.txt' INTO TABLE src_null;

INSERT OVERWRITE TABLE test_orc SELECT a FROM src_null;

SELECT * FROM test_orc LIMIT 5;
