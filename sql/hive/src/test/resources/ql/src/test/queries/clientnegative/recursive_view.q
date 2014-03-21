-- Can't have recursive views

drop table t;
drop view r0;
drop view r1;
drop view r2;
drop view r3;
create table t (id int);
create view r0 as select * from t;
create view r1 as select * from r0;
create view r2 as select * from r1;
create view r3 as select * from r2;
drop view r0;
alter view r3 rename to r0;
select * from r0;