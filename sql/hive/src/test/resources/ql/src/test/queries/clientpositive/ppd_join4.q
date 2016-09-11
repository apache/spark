create table dual(a string);

set hive.optimize.ppd=true;
drop table if exists test_tbl ;

create table test_tbl (id string,name string);

insert into table test_tbl
select 'a','b' from dual;

explain
select t2.* 
from
(select id,name from (select id,name from test_tbl) t1 sort by id) t2
join test_tbl t3 on (t2.id=t3.id )
where t2.name='c' and t3.id='a';

select t2.* 
from
(select id,name from (select id,name from test_tbl) t1 sort by id) t2
join test_tbl t3 on (t2.id=t3.id )
where t2.name='c' and t3.id='a';