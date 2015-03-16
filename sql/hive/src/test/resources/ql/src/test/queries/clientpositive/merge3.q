set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table merge_src as 
select key, value from srcpart where ds is not null;

create table merge_src_part (key string, value string) partitioned by (ds string);
insert overwrite table merge_src_part partition(ds) select key, value, ds from srcpart where ds is not null;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

explain extended
create table merge_src2 as 
select key, value from merge_src;

create table merge_src2 as 
select key, value from merge_src;

select * from merge_src2 ORDER BY key ASC, value ASC;
describe formatted merge_src2;

create table merge_src_part2 like merge_src_part;


explain extended 
insert overwrite table merge_src_part2 partition(ds)
select key, value, ds from merge_src_part
where ds is not null;
 
insert overwrite table merge_src_part2 partition(ds)
select key, value, ds from merge_src_part
where ds is not null;

show partitions merge_src_part2;

select * from merge_src_part2 where ds is not null ORDER BY key ASC, value ASC, ds ASC;

drop table merge_src_part2;

create table merge_src_part2 like merge_src_part;

explain extended
from (select * from merge_src_part where ds is not null distribute by ds) s
insert overwrite table merge_src_part2 partition(ds)
select key, value, ds;

from (select * from merge_src_part where ds is not null distribute by ds) s
insert overwrite table merge_src_part2 partition(ds)
select key, value, ds;

show partitions merge_src_part2;

select * from merge_src_part2 where ds is not null ORDER BY key ASC, value ASC, ds ASC;
