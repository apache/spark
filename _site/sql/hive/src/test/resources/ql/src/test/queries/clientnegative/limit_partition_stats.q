set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.stats.autogather=true;
set hive.compute.query.using.stats=true;

create table part (c int) partitioned by (d string);
insert into table part partition (d)
select hr,ds from srcpart;

set hive.limit.query.max.table.partition=1;

explain select count(*) from part;
select count(*) from part;

set hive.compute.query.using.stats=false;

explain select count(*) from part;
select count(*) from part;
