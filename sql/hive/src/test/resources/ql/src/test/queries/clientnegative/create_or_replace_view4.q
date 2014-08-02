-- View must have at least one non-partition column.

drop view v;
create view v partitioned on (ds, hr) as select * from srcpart;
create or replace view v partitioned on (ds, hr) as select ds, hr from srcpart;