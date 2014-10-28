set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.enforce.bucketing=false;
set hive.enforce.sorting=false;

create table over1k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
           ts timestamp,
           dec decimal(4,2),
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over1k' into table over1k;

create table over1k_orc like over1k;
alter table over1k_orc set fileformat orc;
insert overwrite table over1k_orc select * from over1k;

create table over1k_part_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (ds string, t tinyint) stored as orc;

create table over1k_part_limit_orc like over1k_part_orc;
alter table over1k_part_limit_orc set fileformat orc;

create table over1k_part_buck_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) into 4 buckets stored as orc;

create table over1k_part_buck_sort_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si) 
       sorted by (f) into 4 buckets stored as orc;

-- map-only jobs converted to map-reduce job by hive.optimize.sort.dynamic.partition optimization
explain insert overwrite table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
explain insert overwrite table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
explain insert overwrite table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
explain insert overwrite table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

insert overwrite table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
insert overwrite table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
insert overwrite table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
insert overwrite table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;

-- map-reduce jobs modified by hive.optimize.sort.dynamic.partition optimization
explain insert into table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
explain insert into table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
explain insert into table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
explain insert into table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

insert into table over1k_part_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by si;
insert into table over1k_part_limit_orc partition(ds="foo", t) select si,i,b,f,t from over1k_orc where t is null or t=27 limit 10;
insert into table over1k_part_buck_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
insert into table over1k_part_buck_sort_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

desc formatted over1k_part_orc partition(ds="foo",t=27);
desc formatted over1k_part_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_limit_orc partition(ds="foo",t=27);
desc formatted over1k_part_limit_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck_orc partition(t=27);
desc formatted over1k_part_buck_orc partition(t="__HIVE_DEFAULT_PARTITION__");
desc formatted over1k_part_buck_sort_orc partition(t=27);
desc formatted over1k_part_buck_sort_orc partition(t="__HIVE_DEFAULT_PARTITION__");

select count(*) from over1k_part_orc;
select count(*) from over1k_part_limit_orc;
select count(*) from over1k_part_buck_orc;
select count(*) from over1k_part_buck_sort_orc;

-- tests for HIVE-6883
create table over1k_part2_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (ds string, t tinyint);

set hive.optimize.sort.dynamic.partition=false;
explain insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;
set hive.optimize.sort.dynamic.partition=true;
explain insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;

set hive.optimize.sort.dynamic.partition=false;
insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;

desc formatted over1k_part2_orc partition(ds="foo",t=27);
desc formatted over1k_part2_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");

select * from over1k_part2_orc;
select count(*) from over1k_part2_orc;

set hive.optimize.sort.dynamic.partition=true;
insert overwrite table over1k_part2_orc partition(ds="foo",t) select si,i,b,f,t from over1k_orc where t is null or t=27 order by i;

desc formatted over1k_part2_orc partition(ds="foo",t=27);
desc formatted over1k_part2_orc partition(ds="foo",t="__HIVE_DEFAULT_PARTITION__");

select * from over1k_part2_orc;
select count(*) from over1k_part2_orc;

-- hadoop-1 does not honor number of reducers in local mode. There is always only 1 reducer irrespective of the number of buckets.
-- Hence all records go to one bucket and all other buckets will be empty. Similar to HIVE-6867. However, hadoop-2 honors number
-- of reducers and records are spread across all reducers. To avoid this inconsistency we will make number of buckets to 1 for this test.
create table over1k_part_buck_sort2_orc(
           si smallint,
           i int,
           b bigint,
           f float)
       partitioned by (t tinyint)
       clustered by (si)
       sorted by (f) into 1 buckets;

set hive.optimize.sort.dynamic.partition=false;
explain insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;
set hive.optimize.sort.dynamic.partition=true;
explain insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

set hive.optimize.sort.dynamic.partition=false;
insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

desc formatted over1k_part_buck_sort2_orc partition(t=27);
desc formatted over1k_part_buck_sort2_orc partition(t="__HIVE_DEFAULT_PARTITION__");

select * from over1k_part_buck_sort2_orc;
select count(*) from over1k_part_buck_sort2_orc;

set hive.optimize.sort.dynamic.partition=true;
insert overwrite table over1k_part_buck_sort2_orc partition(t) select si,i,b,f,t from over1k_orc where t is null or t=27;

desc formatted over1k_part_buck_sort2_orc partition(t=27);
desc formatted over1k_part_buck_sort2_orc partition(t="__HIVE_DEFAULT_PARTITION__");

select * from over1k_part_buck_sort2_orc;
select count(*) from over1k_part_buck_sort2_orc;
