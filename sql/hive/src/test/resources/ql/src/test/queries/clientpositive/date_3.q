drop table date_3;

create table date_3 (
  c1 int
);

alter table date_3 add columns (c2 date);

insert overwrite table date_3
  select 1, cast(cast('2011-01-01 00:00:00' as timestamp) as date) from src tablesample (1 rows);

select * from date_3;

drop table date_3;
