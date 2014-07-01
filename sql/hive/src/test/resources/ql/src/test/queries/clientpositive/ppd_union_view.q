-- test predicate pushdown on a view with a union

drop view v;

create table t1_new (key string, value string) partitioned by (ds string);

insert overwrite table t1_new partition (ds = '2011-10-15')
select 'key1', 'value1' from src limit 1;

insert overwrite table t1_new partition (ds = '2011-10-16')
select 'key2', 'value2' from src limit 1;

create table t1_old (keymap string, value string) partitioned by (ds string);

insert overwrite table t1_old partition (ds = '2011-10-13')
select 'keymap3', 'value3' from src limit 1;

insert overwrite table t1_old partition (ds = '2011-10-14')
select 'keymap4', 'value4' from src limit 1;

create table t1_mapping (key string, keymap string) partitioned by (ds string);

insert overwrite table t1_mapping partition (ds = '2011-10-13')
select 'key3', 'keymap3' from src limit 1;

insert overwrite table t1_mapping partition (ds = '2011-10-14')
select 'key4', 'keymap4' from src limit 1;


create view t1 partitioned on (ds) as
select * from
(
select key, value, ds from t1_new
union all
select key, value, t1_old.ds from t1_old join t1_mapping
on t1_old.keymap = t1_mapping.keymap and
   t1_old.ds = t1_mapping.ds
) subq;

explain extended
select * from t1 where ds = '2011-10-13';

select * from t1 where ds = '2011-10-13';

select * from t1 where ds = '2011-10-14';

explain extended
select * from t1 where ds = '2011-10-15';

select * from t1 where ds = '2011-10-15';
select * from t1 where ds = '2011-10-16';