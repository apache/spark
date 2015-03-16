SET hive.vectorized.execution.enabled=true;

SET hive.map.groupby.sorted=true;

create table dtest(a int, b int) clustered by (a) sorted by (a) into 1 buckets stored as orc;
insert into table dtest select c,b from (select array(300,300,300,300,300) as a, 1 as b from src limit 1) y lateral view  explode(a) t1 as c;

explain select sum(distinct a), count(distinct a) from dtest;
select sum(distinct a), count(distinct a) from dtest;

explain select sum(distinct cint), count(distinct cint), avg(distinct cint), std(distinct cint) from alltypesorc;
select sum(distinct cint), count(distinct cint), avg(distinct cint), std(distinct cint) from alltypesorc;
