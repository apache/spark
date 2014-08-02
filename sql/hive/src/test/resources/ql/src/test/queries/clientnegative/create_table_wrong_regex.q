drop table aa;
create table aa ( test STRING )
  ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
  WITH SERDEPROPERTIES ("input.regex" = "[^\\](.*)", "output.format.string" = "$1s"); 
