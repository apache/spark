set hive.merge.mapredfiles=true;

create table dest1(key int, val int);

explain
insert overwrite table dest1
select key, count(1) from src group by key;

insert overwrite table dest1
select key, count(1) from src group by key;

select * from dest1;

drop table dest1;

create table test_src(key string, value string) partitioned by (ds string);
create table dest1(key string);

insert overwrite table test_src partition(ds='101') select * from src; 
insert overwrite table test_src partition(ds='102') select * from src;

explain 
insert overwrite table dest1 select key from test_src;
insert overwrite table dest1 select key from test_src;

set hive.merge.smallfiles.avgsize=16;
explain
insert overwrite table dest1 select key from test_src;
insert overwrite table dest1 select key from test_src;
