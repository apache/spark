set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- This tests that the schema can be changed for binary serde data
create table prt(key string, value string) partitioned by (dt string);
insert overwrite table prt partition(dt='1') select * from src where key = 238;

select * from prt where dt is not null;
select key+key, value from prt where dt is not null;

alter table prt add columns (value2 string);

select key+key, value from prt where dt is not null;
select * from prt where dt is not null;
