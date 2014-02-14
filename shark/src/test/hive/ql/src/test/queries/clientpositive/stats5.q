set datanucleus.cache.collections=false;
set hive.stats.autogather=false;

create table analyze_src as select * from src;

explain analyze table analyze_src compute statistics;

analyze table analyze_src compute statistics;

describe formatted analyze_src;
