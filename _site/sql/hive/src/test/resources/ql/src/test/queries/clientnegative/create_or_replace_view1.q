-- Cannot add or drop partition columns with CREATE OR REPLACE VIEW if partitions currently exist (must specify partition columns)

drop view v;
create view v partitioned on (ds, hr) as select * from srcpart;
alter view v add partition (ds='1',hr='2');
create or replace view v as select * from srcpart;