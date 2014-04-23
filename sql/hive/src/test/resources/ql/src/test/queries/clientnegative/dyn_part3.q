set hive.exec.max.dynamic.partitions=600;
set hive.exec.max.dynamic.partitions.pernode=600;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.created.files=100;

create table nzhang_part( key string) partitioned by (value string);

insert overwrite table nzhang_part partition(value) select key, value from src;
