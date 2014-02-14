set datanucleus.cache.collections=false;
set hive.stats.autogather=false;

create table analyze_srcbucket like srcbucket;
insert overwrite table analyze_srcbucket select * from srcbucket;

explain analyze table analyze_srcbucket compute statistics;
analyze table analyze_srcbucket compute statistics;
describe formatted analyze_srcbucket;
