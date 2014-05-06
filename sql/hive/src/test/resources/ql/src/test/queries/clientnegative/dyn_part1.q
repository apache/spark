set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;
set hive.exec.max.dynamic.partitions=2;


create table dynamic_partition (key string) partitioned by (value string);

insert overwrite table dynamic_partition partition(hr) select key, value from src;



