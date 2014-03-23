set hive.stats.autogather=true;
set datanucleus.cache.collections=false;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

CREATE TABLE stats_non_partitioned (key string, value string);

explain extended
insert overwrite table stats_non_partitioned
select * from src;

insert overwrite table stats_non_partitioned
select * from src;

desc extended stats_non_partitioned;

select * from stats_non_partitioned;


CREATE TABLE stats_partitioned(key string, value string) partitioned by (ds string);

explain
insert overwrite table stats_partitioned partition (ds='1')
select * from src;

insert overwrite table stats_partitioned partition (ds='1')
select * from src;

show partitions stats_partitioned;
select * from stats_partitioned where ds is not null;

describe extended stats_partitioned partition (ds='1');
describe extended stats_partitioned;


set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;

drop table stats_non_partitioned;
drop table stats_partitioned;

CREATE TABLE stats_non_partitioned (key string, value string);

explain extended
insert overwrite table stats_non_partitioned
select * from src;

insert overwrite table stats_non_partitioned
select * from src;

desc extended stats_non_partitioned;

select * from stats_non_partitioned;


CREATE TABLE stats_partitioned(key string, value string) partitioned by (ds string);

explain
insert overwrite table stats_partitioned partition (ds='1')
select * from src;

insert overwrite table stats_partitioned partition (ds='1')
select * from src;

show partitions stats_partitioned;
select * from stats_partitioned where ds is not null;

describe extended stats_partitioned partition (ds='1');
describe extended stats_partitioned;
