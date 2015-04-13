create table tab1(c int) partitioned by (i int);
alter table tab1 add partition(i = "some name");

drop table tab1;
