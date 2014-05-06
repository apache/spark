create table t1( key_int1 int, key_int2 int, key_string1 string, key_string2 string);

set hive.optimize.reducededuplication=false;

set hive.map.aggr=false;
select Q1.key_int1, sum(Q1.key_int1) from (select * from t1 cluster by key_int1) Q1 group by Q1.key_int1;

drop table t1;
