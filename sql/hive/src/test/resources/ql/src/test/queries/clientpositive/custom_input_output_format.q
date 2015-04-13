CREATE TABLE src1_rot13_iof(key STRING, value STRING) 
  STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.udf.Rot13InputFormat'
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.udf.Rot13OutputFormat';
DESCRIBE EXTENDED src1_rot13_iof;
SELECT * FROM src1 ORDER BY key, value;
INSERT OVERWRITE TABLE src1_rot13_iof SELECT * FROM src1;
SELECT * FROM src1_rot13_iof ORDER BY key, value;
