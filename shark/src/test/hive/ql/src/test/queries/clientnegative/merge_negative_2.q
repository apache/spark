create table srcpart2 (key int, value string) partitioned by (ds string);
insert overwrite table srcpart2 partition (ds='2011') select * from src;
alter table srcpart2 concatenate;
