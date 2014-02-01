

CREATE TABLE table_test_output_format(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.mapred.MapFileOutputFormat';

FROM src
INSERT OVERWRITE TABLE table_test_output_format SELECT src.key, src.value LIMIT 10;

describe table_test_output_format;

