-- Test SHOW CREATE TABLE on a table with serde.

-- without a storage handler
CREATE TABLE tmp_showcrt1 (key int, value string, newvalue bigint)
COMMENT 'temporary table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';
SHOW CREATE TABLE tmp_showcrt1;
DROP TABLE tmp_showcrt1;

-- with a storage handler and serde properties
CREATE EXTERNAL TABLE tmp_showcrt1 (key string, value boolean)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
WITH SERDEPROPERTIES ('field.delim'=',', 'serialization.format'='$');
SHOW CREATE TABLE tmp_showcrt1;
DROP TABLE tmp_showcrt1;

