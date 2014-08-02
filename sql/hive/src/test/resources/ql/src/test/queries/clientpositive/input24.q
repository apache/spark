
create table tst(a int, b int) partitioned by (d string);
alter table tst add partition (d='2009-01-01');
explain
select count(1) from tst x where x.d='2009-01-01';

select count(1) from tst x where x.d='2009-01-01';


