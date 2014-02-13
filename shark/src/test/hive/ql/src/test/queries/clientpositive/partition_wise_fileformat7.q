set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

create table partition_test_partitioned(key string, value string) partitioned by (dt string);

alter table partition_test_partitioned set fileformat rcfile;
insert overwrite table partition_test_partitioned partition(dt=101) select * from src1;

select count(1) from partition_test_partitioned  a join partition_test_partitioned  b on a.key = b.key
where a.dt = '101' and b.dt = '101';

select count(1) from partition_test_partitioned  a join partition_test_partitioned  b on a.key = b.key
where a.dt = '101' and b.dt = '101' and a.key < 100;