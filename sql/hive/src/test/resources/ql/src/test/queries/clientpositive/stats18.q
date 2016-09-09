set datanucleus.cache.collections=false;
set hive.stats.autogather=true;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.map.aggr=true;

create table stats_part like srcpart;

insert overwrite table stats_part partition (ds='2010-04-08', hr = '13') select key, value from src;

-- Load a file into a existing partition
-- Some stats (numFiles, totalSize) should be updated correctly
-- Some other stats (numRows, rawDataSize) should be cleared
desc formatted stats_part partition (ds='2010-04-08', hr='13');

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE stats_part partition (ds='2010-04-08', hr='13');

desc formatted stats_part partition (ds='2010-04-08', hr='13');

drop table stats_src;
drop table stats_part;
