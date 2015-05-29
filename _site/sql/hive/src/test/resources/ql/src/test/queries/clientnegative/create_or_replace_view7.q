-- Can't update view to have a view cycle (1)

drop view v;
create view v1 partitioned on (ds, hr) as select * from srcpart;
create view v2 partitioned on (ds, hr) as select * from v1;
create view v3 partitioned on (ds, hr) as select * from v2;
create or replace view v1 partitioned on (ds, hr) as select * from v3;