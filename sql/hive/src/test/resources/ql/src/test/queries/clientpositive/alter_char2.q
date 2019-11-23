
-- alter column type, with partitioned table
drop table if exists alter_char2;

create table alter_char2 (
  c1 char(255)
) partitioned by (hr int);

insert overwrite table alter_char2 partition (hr=1)
  select value from src limit 1;

select c1, length(c1) from alter_char2;

alter table alter_char2 change column c1 c1 char(10);

select hr, c1, length(c1) from alter_char2 where hr = 1;

insert overwrite table alter_char2 partition (hr=2)
  select key from src limit 1;

select hr, c1, length(c1) from alter_char2 where hr = 1;
select hr, c1, length(c1) from alter_char2 where hr = 2;
