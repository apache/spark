
create table altern1(a int, b int) partitioned by (ds string);
alter table altern1 replace columns(a int, b int, ds string);

