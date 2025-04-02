set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- This tests that the schema can be changed for partitioned tables for binary serde data for joins
create table T1(key string, value string) partitioned by (dt string) stored as rcfile;
alter table T1 set serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';
insert overwrite table T1 partition (dt='1') select * from src where key = 238 or key = 97;

alter table T1 change key key int;
insert overwrite table T1 partition (dt='2') select * from src where key = 238 or key = 97;

alter table T1 change key key string;

create table T2(key string, value string) partitioned by (dt string) stored as rcfile;
insert overwrite table T2 partition (dt='1') select * from src where key = 238 or key = 97;

select /* + MAPJOIN(a) */ count(*) FROM T1 a JOIN T2 b ON a.key = b.key;
select count(*) FROM T1 a JOIN T2 b ON a.key = b.key;