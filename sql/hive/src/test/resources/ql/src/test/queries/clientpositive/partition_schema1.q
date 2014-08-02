
create table partition_schema1(key string, value string) partitioned by (dt string);

insert overwrite table partition_schema1 partition(dt='100') select * from src1;
desc partition_schema1 partition(dt='100');

alter table partition_schema1 add columns (x string);

desc partition_schema1;
desc partition_schema1 partition (dt='100');


