set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

create table partition_test_partitioned(key string, value string) partitioned by (dt string);

alter table partition_test_partitioned set fileformat rcfile;
insert overwrite table partition_test_partitioned partition(dt=101) select * from src1;
alter table partition_test_partitioned set fileformat Sequencefile;

insert overwrite table partition_test_partitioned partition(dt=102) select * from src1;

select count(1) from
(select key, value from partition_test_partitioned where dt=101 and key < 100
 union all
select key, value from partition_test_partitioned where dt=101 and key < 20)s;

select count(1) from
(select key, value from partition_test_partitioned where dt=101 and key < 100
 union all
select key, value from partition_test_partitioned where dt=102 and key < 20)s;
