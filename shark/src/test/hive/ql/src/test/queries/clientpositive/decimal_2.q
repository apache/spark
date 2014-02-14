drop table decimal_2;

create table decimal_2 (t decimal);
alter table decimal_2 set serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';

insert overwrite table decimal_2
  select cast('17.29' as decimal) from src limit 1;

select cast(t as boolean) from decimal_2 limit 1;
select cast(t as tinyint) from decimal_2 limit 1;
select cast(t as smallint) from decimal_2 limit 1;
select cast(t as int) from decimal_2 limit 1;
select cast(t as bigint) from decimal_2 limit 1;
select cast(t as float) from decimal_2 limit 1;
select cast(t as double) from decimal_2 limit 1;
select cast(t as string) from decimal_2 limit 1;

insert overwrite table decimal_2
  select cast('3404045.5044003' as decimal) from src limit 1;

select cast(t as boolean) from decimal_2 limit 1;
select cast(t as tinyint) from decimal_2 limit 1;
select cast(t as smallint) from decimal_2 limit 1;
select cast(t as int) from decimal_2 limit 1;
select cast(t as bigint) from decimal_2 limit 1;
select cast(t as float) from decimal_2 limit 1;
select cast(t as double) from decimal_2 limit 1;
select cast(t as string) from decimal_2 limit 1;

select cast(3.14 as decimal) from decimal_2 limit 1;
select cast(cast(3.14 as float) as decimal) from decimal_2 limit 1;
select cast(cast('2012-12-19 11:12:19.1234567' as timestamp) as decimal) from decimal_2 limit 1;
select cast(true as decimal) from decimal_2 limit 1;
select cast(3Y as decimal) from decimal_2 limit 1;
select cast(3S as decimal) from decimal_2 limit 1;
select cast(cast(3 as int) as decimal) from decimal_2 limit 1;
select cast(3L as decimal) from decimal_2 limit 1;
select cast(0.99999999999999999999 as decimal) from decimal_2 limit 1;
select cast('0.99999999999999999999' as decimal) from decimal_2 limit 1;
drop table decimal_2;
