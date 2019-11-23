
create table tst(a int, b int) partitioned by (d string);
alter table tst add partition (d='2009-01-01');
alter table tst add partition (d='2009-02-02');

explain
select * from (
  select * from tst x where x.d='2009-01-01' limit 10
    union all
  select * from tst x where x.d='2009-02-02' limit 10
) subq;

select * from (
  select * from tst x where x.d='2009-01-01' limit 10
    union all
  select * from tst x where x.d='2009-02-02' limit 10
) subq;


