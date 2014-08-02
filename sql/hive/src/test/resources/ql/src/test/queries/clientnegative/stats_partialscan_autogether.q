set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.min.split.size=256;
set mapred.min.split.size.per.node=256;
set mapred.min.split.size.per.rack=256;
set mapred.max.split.size=256;

-- test analyze table ... compute statistics partialscan

-- 1. prepare data
CREATE table analyze_srcpart_partial_scan (key STRING, value STRING)
partitioned by (ds string, hr string)
stored as rcfile;
insert overwrite table analyze_srcpart_partial_scan partition (ds, hr) select * from srcpart where ds is not null;
describe formatted analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11);


-- 2. partialscan
explain
analyze table analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11) compute statistics partialscan;
analyze table analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11) compute statistics partialscan;

-- 3. confirm result
describe formatted analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11);
describe formatted analyze_srcpart_partial_scan PARTITION(ds='2008-04-09',hr=11);
drop table analyze_srcpart_partial_scan;



