set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;

create table t1 (id int, key string, value string);
create table t2 (id int, key string, value string);
create table t3 (id int, key string, value string);
create table t4 (id int, key string, value string);

explain select * from t1 full outer join t2 on t1.id=t2.id join t3 on t2.id=t3.id where t3.id=20;
explain select * from t1 join t2 on (t1.id=t2.id) left outer join t3 on (t2.id=t3.id) where t2.id=20;
explain select * from t1 join t2 on (t1.id=t2.id) left outer join t3 on (t1.id=t3.id) where t2.id=20;

drop table t1;
drop table t2;
drop table t3;
drop table t4;