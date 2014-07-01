drop table pcr_t1;
drop table pcr_t2;
drop table pcr_t3;

create table pcr_t1 (key int, value string) partitioned by (ds string);

insert overwrite table pcr_t1 partition (ds='2000-04-08') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds='2000-04-09') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds='2000-04-10') select * from src where key < 20 order by key;

explain extended select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 order by key, ds;
select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 order by key, ds;

explain extended select key, value from pcr_t1 where ds<='2000-04-09' or key<5 order by key;
select key, value from pcr_t1 where ds<='2000-04-09' or key<5 order by key;

explain extended select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 and value != 'val_2' order by key, ds;
select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 and value != 'val_2' order by key, ds;


explain extended
select key, value, ds from pcr_t1
where (ds < '2000-04-09' and key < 5) or (ds > '2000-04-09' and value == 'val_5') order by key, ds;

select key, value, ds from pcr_t1
where (ds < '2000-04-09' and key < 5) or (ds > '2000-04-09' and value == 'val_5') order by key, ds;


explain extended
select key, value, ds from pcr_t1
where (ds < '2000-04-10' and key < 5) or (ds > '2000-04-08' and value == 'val_5') order by key, ds;

select key, value, ds from pcr_t1
where (ds < '2000-04-10' and key < 5) or (ds > '2000-04-08' and value == 'val_5') order by key, ds;


explain extended
select key, value, ds from pcr_t1
where (ds < '2000-04-10' or key < 5) and (ds > '2000-04-08' or value == 'val_5') order by key, ds;

select key, value, ds from pcr_t1
where (ds < '2000-04-10' or key < 5) and (ds > '2000-04-08' or value == 'val_5') order by key, ds;


explain extended select key, value from pcr_t1 where (ds='2000-04-08' or ds='2000-04-09') and key=14 order by key, value;
select key, value from pcr_t1 where (ds='2000-04-08' or ds='2000-04-09') and key=14 order by key, value;

explain extended select key, value from pcr_t1 where ds='2000-04-08' or ds='2000-04-09' order by key, value;
select key, value from pcr_t1 where ds='2000-04-08' or ds='2000-04-09' order by key, value;

explain extended select key, value from pcr_t1 where ds>='2000-04-08' or ds<'2000-04-10' order by key, value;
select key, value from pcr_t1 where ds>='2000-04-08' or ds<'2000-04-10' order by key, value;

explain extended select key, value, ds from pcr_t1 where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2) order by key, value, ds;
select key, value, ds from pcr_t1 where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2) order by key, value, ds;

explain extended select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08' order by t1.key;
select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08' order by t1.key;

explain extended select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-09' order by t1.key;
select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-09' order by t1.key;

insert overwrite table pcr_t1 partition (ds='2000-04-11') select * from src where key < 20 order by key;

explain extended select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds>='2000-04-08' and ds<='2000-04-11' and key=2) order by key, value, ds;
select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds>='2000-04-08' and ds<='2000-04-11' and key=2) order by key, value, ds;

explain extended select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds<='2000-04-09' and key=2) order by key, value, ds;
select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds<='2000-04-09' and key=2) order by key, value, ds;

create table pcr_t2 (key int, value string);
create table pcr_t3 (key int, value string);

explain extended
from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08'
insert overwrite table pcr_t3 select key, value where ds='2000-04-08';

from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08'
insert overwrite table pcr_t3 select key, value where ds='2000-04-08';

explain extended
from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08' and key=2
insert overwrite table pcr_t3 select key, value where ds='2000-04-08' and key=3;

from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08' and key=2
insert overwrite table pcr_t3 select key, value where ds='2000-04-08' and key=3;


explain extended select key, value from srcpart where ds='2008-04-08' and hr=11 order by key limit 10;
select key, value from srcpart where ds='2008-04-04' and hr=11 order by key limit 10;

explain extended select key, value, ds, hr from srcpart where ds='2008-04-08' and (hr='11' or hr='12') and key=11 order by key, ds, hr;
select key, value, ds, hr from srcpart where ds='2008-04-08' and (hr='11' or hr='12') and key=11 order by key, ds, hr;

explain extended select key, value, ds, hr from srcpart where hr='11' and key=11 order by key, ds, hr;
select key, value, ds, hr from srcpart where hr='11' and key=11 order by key, ds, hr;

drop table pcr_t1;
drop table pcr_t2;
drop table pcr_t3;


-- Test cases when a non-boolean ds expression has same and different values for all possible ds values: 
drop table pcr_foo;
create table pcr_foo (key int, value string) partitioned by (ds int);
insert overwrite table pcr_foo partition (ds=3) select * from src where key < 10 order by key;
insert overwrite table pcr_foo partition (ds=5) select * from src where key < 10 order by key;
insert overwrite table pcr_foo partition (ds=7) select * from src where key < 10 order by key;

-- the condition is 'true' for all the 3 partitions (ds=3,5,7):
select key, value, ds from pcr_foo where (ds % 2 == 1);

-- the condition is 'true' for partitions (ds=3,5) but 'false' of partition ds=7:
select key, value, ds from pcr_foo where (ds / 3 < 2);

drop table pcr_foo;



-- Cover org.apache.hadoop.hive.ql.optimizer.pcr.PcrExprProcFactory.FieldExprProcessor.
-- Create a table with a struct data:
create table ab(strct struct<a:int, b:string>)
row format delimited
  fields terminated by '\t'
  collection items terminated by '\001';
load data local inpath '../data/files/kv1.txt'
overwrite into table ab;

-- Create partitioned table with struct data:
drop table foo_field;
create table foo_field (s struct<a:int,b:string>) partitioned by (ds int);
insert overwrite table foo_field partition (ds=5) select strct from ab where strct.a < 10 limit 2;
insert overwrite table foo_field partition (ds=7) select strct from ab where strct.a > 190 limit 2;
select s,ds from foo_field where ((ds + s.a) > 0) order by ds,s;

drop table foo_field;

