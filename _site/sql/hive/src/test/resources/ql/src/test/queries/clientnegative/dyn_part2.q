
create table nzhang_part1 (key string, value string) partitioned by (ds string, hr string);

set hive.exec.dynamic.partition=true;

insert overwrite table nzhang_part1 partition(ds='11', hr) select key, value from srcpart where ds is not null;

show partitions nzhang_part1;



