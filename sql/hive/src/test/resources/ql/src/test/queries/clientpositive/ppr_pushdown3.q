set hive.mapred.mode=nonstrict;

explain select * from srcpart where key < 10;
select * from srcpart where key < 10;

explain select * from srcpart;
select * from srcpart;

explain select key from srcpart;
select key from srcpart;
