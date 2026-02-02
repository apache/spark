
create table tst(a string, b string) partitioned by (d string);
alter table tst add partition (d='2009-01-01');

insert overwrite table tst partition(d='2009-01-01')
select tst.a, src.value from tst join src ON (tst.a = src.key);

select * from tst where tst.d='2009-01-01';


