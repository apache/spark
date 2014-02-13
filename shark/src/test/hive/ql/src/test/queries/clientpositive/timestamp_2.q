drop table timestamp_2;

create table timestamp_2 (t timestamp);
alter table timestamp_2 set serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';

insert overwrite table timestamp_2 
  select cast('2011-01-01 01:01:01' as timestamp) from src limit 1;
select cast(t as boolean) from timestamp_2 limit 1;
select cast(t as tinyint) from timestamp_2 limit 1;
select cast(t as smallint) from timestamp_2 limit 1;
select cast(t as int) from timestamp_2 limit 1;
select cast(t as bigint) from timestamp_2 limit 1;
select cast(t as float) from timestamp_2 limit 1;
select cast(t as double) from timestamp_2 limit 1;
select cast(t as string) from timestamp_2 limit 1;

insert overwrite table timestamp_2
  select '2011-01-01 01:01:01' from src limit 1;
select cast(t as boolean) from timestamp_2 limit 1;
select cast(t as tinyint) from timestamp_2 limit 1;
select cast(t as smallint) from timestamp_2 limit 1;
select cast(t as int) from timestamp_2 limit 1;
select cast(t as bigint) from timestamp_2 limit 1;
select cast(t as float) from timestamp_2 limit 1;
select cast(t as double) from timestamp_2 limit 1;
select cast(t as string) from timestamp_2 limit 1;

insert overwrite table timestamp_2
  select '2011-01-01 01:01:01.1' from src limit 1;
select cast(t as boolean) from timestamp_2 limit 1;
select cast(t as tinyint) from timestamp_2 limit 1;
select cast(t as smallint) from timestamp_2 limit 1;
select cast(t as int) from timestamp_2 limit 1;
select cast(t as bigint) from timestamp_2 limit 1;
select cast(t as float) from timestamp_2 limit 1;
select cast(t as double) from timestamp_2 limit 1;
select cast(t as string) from timestamp_2 limit 1;

insert overwrite table timestamp_2
  select '2011-01-01 01:01:01.0001' from src limit 1;
select cast(t as boolean) from timestamp_2 limit 1;
select cast(t as tinyint) from timestamp_2 limit 1;
select cast(t as smallint) from timestamp_2 limit 1;
select cast(t as int) from timestamp_2 limit 1;
select cast(t as bigint) from timestamp_2 limit 1;
select cast(t as float) from timestamp_2 limit 1;
select cast(t as double) from timestamp_2 limit 1;
select cast(t as string) from timestamp_2 limit 1;

insert overwrite table timestamp_2
  select '2011-01-01 01:01:01.000100000' from src limit 1;
select cast(t as boolean) from timestamp_2 limit 1;
select cast(t as tinyint) from timestamp_2 limit 1;
select cast(t as smallint) from timestamp_2 limit 1;
select cast(t as int) from timestamp_2 limit 1;
select cast(t as bigint) from timestamp_2 limit 1;
select cast(t as float) from timestamp_2 limit 1;
select cast(t as double) from timestamp_2 limit 1;
select cast(t as string) from timestamp_2 limit 1;

insert overwrite table timestamp_2
  select '2011-01-01 01:01:01.001000011' from src limit 1;
select cast(t as boolean) from timestamp_2 limit 1;
select cast(t as tinyint) from timestamp_2 limit 1;
select cast(t as smallint) from timestamp_2 limit 1;
select cast(t as int) from timestamp_2 limit 1;
select cast(t as bigint) from timestamp_2 limit 1;
select cast(t as float) from timestamp_2 limit 1;
select cast(t as double) from timestamp_2 limit 1;
select cast(t as string) from timestamp_2 limit 1;

drop table timestamp_2;
