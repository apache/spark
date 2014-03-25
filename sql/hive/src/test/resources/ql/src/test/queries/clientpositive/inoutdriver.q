create table test (a int) stored as inputformat 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' outputformat 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' inputdriver 'RCFileInDriver' outputdriver 'RCFileOutDriver';
desc extended test;
