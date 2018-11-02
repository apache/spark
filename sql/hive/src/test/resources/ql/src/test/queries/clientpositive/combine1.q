set hive.exec.compress.output = true;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize=256;
set mapreduce.input.fileinputformat.split.minsize.per.node=256;
set mapreduce.input.fileinputformat.split.minsize.per.rack=256;
set mapreduce.input.fileinputformat.split.maxsize=256;

set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

create table combine1_1(key string, value string) stored as textfile;

insert overwrite table combine1_1
select * from src;


select key, value from combine1_1 ORDER BY key ASC, value ASC;

