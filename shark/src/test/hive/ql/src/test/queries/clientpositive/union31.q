drop table t1;
drop table t2;


create table t1 as select * from src where key < 10;
create table t2 as select * from src where key < 10;

create table t3(key string, cnt int);
create table t4(value string, cnt int);

explain
from
(select * from t1
 union all
 select * from t2
) x
insert overwrite table t3
  select key, count(1) group by key
insert overwrite table t4
  select value, count(1) group by value;

from
(select * from t1
 union all
 select * from t2
) x
insert overwrite table t3
  select key, count(1) group by key
insert overwrite table t4
  select value, count(1) group by value;

select * from t3 order by key;
select * from t4 order by value;

create table t5(c1 string, cnt int);
create table t6(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, count(1) as cnt from t2 group by key
) x
insert overwrite table t5
  select c1, sum(cnt) group by c1
insert overwrite table t6
  select c1, sum(cnt) group by c1;

from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, count(1) as cnt from t2 group by key
) x
insert overwrite table t5
  select c1, sum(cnt) group by c1
insert overwrite table t6
  select c1, sum(cnt) group by c1;

select * from t5 order by c1;
select * from t6 order by c1;

drop table t1;
drop table t2;

create table t1 as select * from src where key < 10;
create table t2 as select key, count(1) as cnt from src where key < 10 group by key;

create table t7(c1 string, cnt int);
create table t8(c1 string, cnt int);

explain
from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, cnt from t2
) x
insert overwrite table t7
  select c1, count(1) group by c1
insert overwrite table t8
  select c1, count(1) group by c1;

from
(
 select key as c1, count(1) as cnt from t1 group by key
   union all
 select key as c1, cnt from t2
) x
insert overwrite table t7
  select c1, count(1) group by c1
insert overwrite table t8
  select c1, count(1) group by c1;

select * from t7 order by c1;
select * from t8 order by c1;
