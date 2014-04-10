drop table timestamp_3;

create table timestamp_3 (t timestamp);
alter table timestamp_3 set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

insert overwrite table timestamp_3 
  select cast(cast('1.3041352164485E9' as double) as timestamp) from src limit 1;
select cast(t as boolean) from timestamp_3 limit 1;
select cast(t as tinyint) from timestamp_3 limit 1;
select cast(t as smallint) from timestamp_3 limit 1;
select cast(t as int) from timestamp_3 limit 1;
select cast(t as bigint) from timestamp_3 limit 1;
select cast(t as float) from timestamp_3 limit 1;
select cast(t as double) from timestamp_3 limit 1;
select cast(t as string) from timestamp_3 limit 1;

drop table timestamp_3;
