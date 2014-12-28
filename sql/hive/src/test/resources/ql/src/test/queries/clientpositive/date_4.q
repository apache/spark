set hive.fetch.task.conversion=more;

drop table date_4;

create table date_4 (d date);
alter table date_4 set serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

-- Test date literal syntax
insert overwrite table date_4 
  select date '2011-01-01' from src tablesample (1 rows);
select d, date '2011-01-01' from date_4 limit 1;

drop table date_4;
