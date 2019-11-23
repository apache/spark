drop view v;
create view v as select * from srcpart;
describe formatted v;

-- modifying definition of unpartitioned view
create or replace view v partitioned on (ds, hr) as select * from srcpart;
alter view v add partition (ds='2008-04-08',hr='11');
alter view v add partition (ds='2008-04-08',hr='12');
select * from v where value='val_409' and ds='2008-04-08' and hr='11';
describe formatted v;
show partitions v;

-- altering partitioned view 1
create or replace view v partitioned on (ds, hr) as select value, ds, hr from srcpart;
select * from v where value='val_409' and ds='2008-04-08' and hr='11';
describe formatted v;
show partitions v;

-- altering partitioned view 2
create or replace view v partitioned on (ds, hr) as select key, value, ds, hr from srcpart;
select * from v where value='val_409' and ds='2008-04-08' and hr='11';
describe formatted v;
show partitions v;
drop view v;

-- updating to fix view with invalid definition
create table srcpart_temp like srcpart;
create view v partitioned on (ds, hr) as select * from srcpart_temp;
drop table srcpart_temp; -- v is now invalid
create or replace view v partitioned on (ds, hr) as select * from srcpart;
describe formatted v;
drop view v;