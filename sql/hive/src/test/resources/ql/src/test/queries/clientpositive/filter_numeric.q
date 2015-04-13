set hive.exec.dynamic.partition.mode=nonstrict;

create table partint(key string, value string) partitioned by (ds string, hr int);
insert overwrite table partint partition(ds, hr) select * from srcpart where ds = '2008-04-08';

explain select key, value, hr from partint where hr < 11;
select key, value, hr from partint where hr < 11;

explain select key, value, hr from partint where hr <= 12 and hr > 11;
select key, value, hr from partint where hr <= 12 and hr > 11;

explain select key, value, hr from partint where hr between 11 and 12;
select key, value, hr from partint where hr between 11 and 12;

explain select key, value, hr from partint where hr not between 12 and 14;
select key, value, hr from partint where hr not between 12 and 14;

explain select key, value, hr from partint where hr < 13;
select key, value, hr from partint where hr < 13;

drop table partint;