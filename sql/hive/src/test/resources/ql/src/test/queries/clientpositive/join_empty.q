create table srcpart_empty(key int, value string) partitioned by (ds string);
create table src2_empty (key int, value string);

select /*+mapjoin(a)*/ a.key, b.value from srcpart_empty a join src b on a.key=b.key;
select /*+mapjoin(a)*/ a.key, b.value from src2_empty a join src b on a.key=b.key;

set hive.mapred.mode=nonstrict;
set hive.auto.convert.join = true;
select a.key, b.value from srcpart_empty a join src b on a.key=b.key;
select a.key, b.value from src2_empty a join src b on a.key=b.key;