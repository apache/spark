-- Cannot add or drop partition columns with CREATE OR REPLACE VIEW if partitions currently exist

drop view v;
create view v partitioned on (ds, hr) as select * from srcpart;
alter view v add partition (ds='1',hr='2');
create or replace view v partitioned on (hr) as select * from srcpart;