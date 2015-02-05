
-- alter column type, with partitioned table
drop table if exists alter_varchar2;

create table alter_varchar2 (
  c1 varchar(255)
) partitioned by (hr int);

insert overwrite table alter_varchar2 partition (hr=1)
  select value from src tablesample (1 rows);

select c1, length(c1) from alter_varchar2;

alter table alter_varchar2 change column c1 c1 varchar(10);

select hr, c1, length(c1) from alter_varchar2 where hr = 1;

insert overwrite table alter_varchar2 partition (hr=2)
  select key from src tablesample (1 rows);

set hive.fetch.task.conversion=more;

select hr, c1, length(c1) from alter_varchar2 where hr = 1;
select hr, c1, length(c1) from alter_varchar2 where hr = 2;
