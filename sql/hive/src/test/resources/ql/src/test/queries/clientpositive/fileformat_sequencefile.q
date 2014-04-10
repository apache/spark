EXPLAIN
CREATE TABLE dest1(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat';

CREATE TABLE dest1(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat';

DESCRIBE EXTENDED dest1;

FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.key, src.value WHERE src.key < 10;

SELECT dest1.* FROM dest1;


