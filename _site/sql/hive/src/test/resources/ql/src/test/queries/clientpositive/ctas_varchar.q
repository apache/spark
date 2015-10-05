drop table ctas_varchar_1;
drop table ctas_varchar_2;
drop view ctas_varchar_3;

create table ctas_varchar_1 (key varchar(10), value string);
insert overwrite table ctas_varchar_1 
  select key, value from src sort by key, value limit 5;

-- create table as with varchar column
create table ctas_varchar_2 as select key, value from ctas_varchar_1;

-- view with varchar column
create view ctas_varchar_3 as select key, value from ctas_varchar_2;

select key, value from ctas_varchar_1;
select * from ctas_varchar_2;
select * from ctas_varchar_3;


drop table ctas_varchar_1;
drop table ctas_varchar_2;
drop view ctas_varchar_3;
