-- Can't update view to have an invalid definition

drop view v;
create view v partitioned on (ds, hr) as select * from srcpart;
create or replace view v partitioned on (ds, hr) as blah;