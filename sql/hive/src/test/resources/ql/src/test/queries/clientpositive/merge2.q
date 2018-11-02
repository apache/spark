set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapreduce.input.fileinputformat.split.minsize=256;
set mapreduce.input.fileinputformat.split.minsize.per.node=256;
set mapreduce.input.fileinputformat.split.minsize.per.rack=256;
set mapreduce.input.fileinputformat.split.maxsize=256;

create table test1(key int, val int);

explain
insert overwrite table test1
select key, count(1) from src group by key;

insert overwrite table test1
select key, count(1) from src group by key;

select * from test1;

drop table test1;


create table test_src(key string, value string) partitioned by (ds string);
create table test1(key string);

insert overwrite table test_src partition(ds='101') select * from src; 
insert overwrite table test_src partition(ds='102') select * from src;

explain 
insert overwrite table test1 select key from test_src;
insert overwrite table test1 select key from test_src;

set hive.merge.smallfiles.avgsize=16;
explain
insert overwrite table test1 select key from test_src;
insert overwrite table test1 select key from test_src;
