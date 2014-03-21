
CREATE table columnTable (key STRING, value STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
STORED AS
  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat';

FROM src
INSERT OVERWRITE TABLE columnTable SELECT src.key, src.value LIMIT 10;
describe columnTable;

SELECT columnTable.* FROM columnTable ORDER BY key ASC, value ASC;


