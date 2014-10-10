set hive.exec.compress.output = true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.min.split.size=256;
set mapred.min.split.size.per.node=256;
set mapred.min.split.size.per.rack=256;
set mapred.max.split.size=256;

set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

create table combine1_1(key string, value string) stored as textfile;

insert overwrite table combine1_1
select * from src;


select key, value from combine1_1 ORDER BY key ASC, value ASC;

