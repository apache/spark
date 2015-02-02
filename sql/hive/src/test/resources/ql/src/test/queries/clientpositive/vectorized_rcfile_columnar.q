--This query must pass even when vectorized reader is not available for
--RC files. The query must fall back to the non-vector mode and run successfully.

CREATE table columnTable (key STRING, value STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';

FROM src
INSERT OVERWRITE TABLE columnTable SELECT src.key, src.value LIMIT 10;
describe columnTable;

SET hive.vectorized.execution.enabled=true;

SELECT key, value FROM columnTable ORDER BY key;

