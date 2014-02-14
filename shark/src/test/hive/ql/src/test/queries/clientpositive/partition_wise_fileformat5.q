set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

create table partition_test_partitioned(key string, value string) partitioned by (dt string);

alter table partition_test_partitioned set fileformat rcfile;
insert overwrite table partition_test_partitioned partition(dt=101) select * from src1;
alter table partition_test_partitioned set fileformat Sequencefile;
insert overwrite table partition_test_partitioned partition(dt=102) select * from src1;

select dt, count(1) from partition_test_partitioned where dt is not null group by dt;

insert overwrite table partition_test_partitioned partition(dt=103) select * from src1;

select dt, count(1) from partition_test_partitioned where dt is not null group by dt;
