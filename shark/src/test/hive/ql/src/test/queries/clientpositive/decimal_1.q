drop table decimal_1;

create table decimal_1 (t decimal);
alter table decimal_1 set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

insert overwrite table decimal_1
  select cast('17.29' as decimal) from src limit 1;
select cast(t as boolean) from decimal_1 limit 1;
select cast(t as tinyint) from decimal_1 limit 1;
select cast(t as smallint) from decimal_1 limit 1;
select cast(t as int) from decimal_1 limit 1;
select cast(t as bigint) from decimal_1 limit 1;
select cast(t as float) from decimal_1 limit 1;
select cast(t as double) from decimal_1 limit 1;
select cast(t as string) from decimal_1 limit 1;
select cast(t as timestamp) from decimal_1 limit 1;

drop table decimal_1;
