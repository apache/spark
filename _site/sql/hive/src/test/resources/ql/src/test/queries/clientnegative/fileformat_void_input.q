CREATE TABLE dest1(key INT, value STRING) STORED AS
  INPUTFORMAT 'java.lang.Void'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 10;

SELECT dest1.* FROM dest1;
