set datanucleus.cache.collections=false;
set hive.stats.autogather=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.input.fileinputformat.split.minsize=256;
set mapreduce.input.fileinputformat.split.minsize.per.node=256;
set mapreduce.input.fileinputformat.split.minsize.per.rack=256;
set mapreduce.input.fileinputformat.split.maxsize=256;

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20,0.20S)
-- This test uses mapreduce.input.fileinputformat.split.maxsize/mapred.max.split.size for controlling
-- number of input splits, which is not effective in hive 0.20.
-- stats_partscan_1_23.q is the same test with this but has different result.

-- test analyze table ... compute statistics partialscan

-- 1. prepare data
CREATE table analyze_srcpart_partial_scan (key STRING, value STRING)
partitioned by (ds string, hr string)
stored as rcfile;
insert overwrite table analyze_srcpart_partial_scan partition (ds, hr) select * from srcpart where ds is not null;
describe formatted analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11);

set hive.stats.autogather=true;

-- 2. partialscan
explain
analyze table analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11) compute statistics partialscan;
analyze table analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11) compute statistics partialscan;

-- 3. confirm result
describe formatted analyze_srcpart_partial_scan PARTITION(ds='2008-04-08',hr=11);
describe formatted analyze_srcpart_partial_scan PARTITION(ds='2008-04-09',hr=11);
drop table analyze_srcpart_partial_scan;



