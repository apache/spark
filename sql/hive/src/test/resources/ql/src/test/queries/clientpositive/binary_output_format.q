-- Create a table with binary output format
CREATE TABLE dest1(mydata STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.last.column.takes.rest'='true'
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveBinaryOutputFormat';

-- Insert into that table using transform
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1
SELECT TRANSFORM(*)
  USING 'cat'
  AS mydata STRING
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'serialization.last.column.takes.rest'='true'
    )
    RECORDREADER 'org.apache.hadoop.hive.ql.exec.BinaryRecordReader'
FROM src;

INSERT OVERWRITE TABLE dest1
SELECT TRANSFORM(*)
  USING 'cat'
  AS mydata STRING
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    WITH SERDEPROPERTIES (
      'serialization.last.column.takes.rest'='true'
    )
    RECORDREADER 'org.apache.hadoop.hive.ql.exec.BinaryRecordReader'
FROM src;

-- Test the result
SELECT * FROM dest1;
