drop table alter_char_1;

create table alter_char_1 (key string, value string);
insert overwrite table alter_char_1
  select key, value from src order by key limit 5;

select * from alter_char_1 order by key;

-- change column to char
alter table alter_char_1 change column value value char(20);
-- contents should still look the same
select * from alter_char_1 order by key;

-- change column to smaller char
alter table alter_char_1 change column value value char(3);
-- value column should be truncated now
select * from alter_char_1 order by key;

-- change back to bigger char
alter table alter_char_1 change column value value char(20);
-- column values should be full size again
select * from alter_char_1 order by key;

-- add char column
alter table alter_char_1 add columns (key2 int, value2 char(10));
select * from alter_char_1 order by key;

insert overwrite table alter_char_1
  select key, value, key, value from src order by key limit 5;
select * from alter_char_1 order by key;

drop table alter_char_1;
