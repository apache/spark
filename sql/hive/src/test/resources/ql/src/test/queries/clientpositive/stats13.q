set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table analyze_srcpart like srcpart;
insert overwrite table analyze_srcpart partition (ds, hr) select * from srcpart where ds is not null;

explain extended
analyze table analyze_srcpart PARTITION(ds='2008-04-08',hr=11) compute statistics;

analyze table analyze_srcpart PARTITION(ds='2008-04-08',hr=11) compute statistics;

desc formatted analyze_srcpart;
desc formatted analyze_srcpart partition (ds='2008-04-08', hr=11);
desc formatted analyze_srcpart partition (ds='2008-04-08', hr=12);
desc formatted analyze_srcpart partition (ds='2008-04-09', hr=11);
desc formatted analyze_srcpart partition (ds='2008-04-09', hr=12);

create table analyze_srcpart2 like analyze_srcpart;

desc formatted analyze_srcpart2;
