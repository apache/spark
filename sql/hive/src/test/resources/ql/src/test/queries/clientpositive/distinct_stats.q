set hive.stats.autogather=true;

set hive.compute.query.using.stats=true;
create table t1 (a string, b string);

insert into table t1 select * from src;

analyze table t1 compute statistics for columns a,b;

explain 
select count(distinct b) from t1 group by a;

explain 
select distinct(b) from t1;

explain 
select a, count(*) from t1 group by a;

drop table t1;
set hive.compute.query.using.stats = false;
