set hive.stats.autogather=true;
set datanucleus.cache.collections=false;

set hive.stats.collect.rawdatasize=true;
CREATE TABLE stats_partitioned(key string, value string) partitioned by (ds string);
insert overwrite table stats_partitioned partition (ds='1')
select * from src;
-- rawDataSize is 5312 after config is turned on
describe formatted stats_partitioned;

set hive.stats.collect.rawdatasize=false;
insert overwrite table stats_partitioned partition (ds='1')
select * from src;
-- rawDataSize is 0 after config is turned off
describe formatted stats_partitioned;

