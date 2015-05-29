drop table ctas_char_1;
drop table ctas_char_2;
drop view ctas_char_3;

create table ctas_char_1 (key char(10), value string);
insert overwrite table ctas_char_1 
  select key, value from src sort by key, value limit 5;

-- create table as with char column
create table ctas_char_2 as select key, value from ctas_char_1;

-- view with char column
create view ctas_char_3 as select key, value from ctas_char_2;

select key, value from ctas_char_1;
select * from ctas_char_2;
select * from ctas_char_3;


drop table ctas_char_1;
drop table ctas_char_2;
drop view ctas_char_3;
