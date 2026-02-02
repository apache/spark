

CREATE TABLE table_test_output_format(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

FROM src
INSERT OVERWRITE TABLE table_test_output_format SELECT src.key, src.value LIMIT 10;
describe table_test_output_format;



CREATE TABLE table_test_output_format_sequencefile(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat';

FROM src
INSERT OVERWRITE TABLE table_test_output_format_sequencefile SELECT src.key, src.value LIMIT 10;
describe table_test_output_format_sequencefile;



CREATE TABLE table_test_output_format_hivesequencefile(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat';

FROM src
INSERT OVERWRITE TABLE table_test_output_format_hivesequencefile SELECT src.key, src.value LIMIT 10;
describe table_test_output_format_hivesequencefile;

