set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapfiles=false;

create table analyze_t1 like srcpart;


explain
insert overwrite table analyze_t1 partition (ds, hr) select * from srcpart where ds is not null;

insert overwrite table analyze_t1 partition (ds, hr) select * from srcpart where ds is not null;

desc formatted analyze_t1;

explain analyze table analyze_t1 partition (ds, hr) compute statistics;

analyze table analyze_t1 partition (ds, hr) compute statistics;

describe formatted analyze_t1;
