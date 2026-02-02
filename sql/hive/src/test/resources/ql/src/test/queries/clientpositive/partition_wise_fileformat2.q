

create table partition_test_partitioned(key string, value string) partitioned by (dt string);

insert overwrite table partition_test_partitioned partition(dt=100) select * from src1;
alter table partition_test_partitioned set fileformat rcfile;
insert overwrite table partition_test_partitioned partition(dt=101) select * from src1;
alter table partition_test_partitioned set fileformat Sequencefile;
insert overwrite table partition_test_partitioned partition(dt=102) select * from src1;

set hive.fetch.task.conversion=minimal;
explain select *, BLOCK__OFFSET__INSIDE__FILE from partition_test_partitioned where dt >=100 and dt <= 102;
select * from partition_test_partitioned where dt >=100 and dt <= 102;

set hive.fetch.task.conversion=more;
explain select *, BLOCK__OFFSET__INSIDE__FILE from partition_test_partitioned where dt >=100 and dt <= 102;
select * from partition_test_partitioned where dt >=100 and dt <= 102;

