-- Can't update view to have a view cycle (2)

drop view v;
create view v1 partitioned on (ds, hr) as select * from srcpart;
create or replace view v1 partitioned on (ds, hr) as select * from v1;