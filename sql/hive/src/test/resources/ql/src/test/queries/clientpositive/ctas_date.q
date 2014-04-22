drop table ctas_date_1;
drop table ctas_date_2;
drop view ctas_date_3;
drop view ctas_date_4;

create table ctas_date_1 (key int, value string, dd date);
insert overwrite table ctas_date_1 
  select key, value, date '2012-01-01' from src sort by key, value limit 5;

-- create table as with date column
create table ctas_date_2 as select key, value, dd, date '1980-12-12' from ctas_date_1;

-- view with date column
create view ctas_date_3 as select * from ctas_date_2 where dd > date '2000-01-01';
create view ctas_date_4 as select * from ctas_date_2 where dd < date '2000-01-01';

select key, value, dd, date '1980-12-12' from ctas_date_1;
select * from ctas_date_2;
select * from ctas_date_3;
select count(*) from ctas_date_4;


drop table ctas_date_1;
drop table ctas_date_2;
drop view ctas_date_3;
drop view ctas_date_4;
