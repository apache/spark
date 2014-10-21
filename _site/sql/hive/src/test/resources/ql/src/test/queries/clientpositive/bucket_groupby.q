create table clustergroupby(key string, value string) partitioned by(ds string);
describe extended clustergroupby;
alter table clustergroupby clustered by (key) into 1 buckets;

insert overwrite table clustergroupby partition (ds='100') select key, value from src sort by key;

explain
select key, count(1) from clustergroupby where ds='100' group by key limit 10;
select key, count(1) from clustergroupby where ds='100' group by key limit 10;

describe extended clustergroupby;
insert overwrite table clustergroupby partition (ds='101') select key, value from src distribute by key;

--normal--
explain
select key, count(1) from clustergroupby  where ds='101'  group by key limit 10;
select key, count(1) from clustergroupby  where ds='101' group by key limit 10;

--function--
explain
select length(key), count(1) from clustergroupby  where ds='101'  group by length(key) limit 10;
select length(key), count(1) from clustergroupby  where ds='101' group by length(key) limit 10;
explain
select abs(length(key)), count(1) from clustergroupby  where ds='101'  group by abs(length(key)) limit 10;
select abs(length(key)), count(1) from clustergroupby  where ds='101' group by abs(length(key)) limit 10;

--constant--
explain
select key, count(1) from clustergroupby  where ds='101'  group by key,3 limit 10;
select key, count(1) from clustergroupby  where ds='101' group by key,3 limit 10;

--subquery--
explain
select key, count(1) from (select value as key, key as value from clustergroupby where ds='101')subq group by key limit 10;
select key, count(1) from (select value as key, key as value from clustergroupby where ds='101')subq group by key limit 10;

explain
select key, count(1) from clustergroupby  group by key;
select key, count(1) from clustergroupby  group by key;

explain
select key, count(1) from clustergroupby  group by key, 3;

-- number of buckets cannot be changed, so drop the table
drop table clustergroupby;
create table clustergroupby(key string, value string) partitioned by(ds string);

--sort columns--
alter table clustergroupby clustered by (value) sorted by (key, value) into 1 buckets;
describe extended clustergroupby;
insert overwrite table clustergroupby partition (ds='102') select key, value from src distribute by value sort by key, value;

explain
select key, count(1) from clustergroupby  where ds='102'  group by key limit 10;
select key, count(1) from clustergroupby  where ds='102' group by key limit 10;
explain
select value, count(1) from clustergroupby  where ds='102'  group by value limit 10;
select value, count(1) from clustergroupby  where ds='102'  group by value limit 10;
explain
select key, count(1) from clustergroupby  where ds='102'  group by key, value limit 10;
select key, count(1) from clustergroupby  where ds='102'  group by key, value limit 10;

-- number of buckets cannot be changed, so drop the table
drop table clustergroupby;
create table clustergroupby(key string, value string) partitioned by(ds string);

alter table clustergroupby clustered by (value, key) sorted by (key) into 1 buckets;
describe extended clustergroupby;
insert overwrite table clustergroupby partition (ds='103') select key, value from src distribute by value, key sort by key;
explain
select key, count(1) from clustergroupby  where ds='103'  group by key limit 10;
select key, count(1) from clustergroupby  where ds='103' group by key limit 10;
explain
select key, count(1) from clustergroupby  where ds='103'  group by value, key limit 10;
select key, count(1) from clustergroupby  where ds='103' group by  value, key limit 10;
