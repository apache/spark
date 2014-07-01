create table nzhang_part4 (key string) partitioned by (ds string, hr string, value string);

set hive.exec.dynamic.partition=true;

insert overwrite table nzhang_part4 partition(value = 'aaa', ds='11', hr) select key, hr from srcpart where ds is not null;

drop table nzhang_part4;
