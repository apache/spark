set datanucleus.cache.collections=false;
set hive.stats.autogather=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- test analyze table ... compute statistics partialscan

create table analyze_srcpart_partial_scan like srcpart;
insert overwrite table analyze_srcpart_partial_scan partition (ds, hr) select * from srcpart where ds is not null;
analyze table analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11) compute statistics partialscan;


