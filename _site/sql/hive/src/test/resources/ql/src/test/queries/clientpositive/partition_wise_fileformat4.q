create table partition_test_partitioned(key string, value string) partitioned by (dt string);
alter table partition_test_partitioned set fileformat sequencefile;
insert overwrite table partition_test_partitioned partition(dt='1') select * from src1;
alter table partition_test_partitioned partition (dt='1') set fileformat sequencefile;

alter table partition_test_partitioned add partition (dt='2');
alter table partition_test_partitioned drop partition (dt='2');

