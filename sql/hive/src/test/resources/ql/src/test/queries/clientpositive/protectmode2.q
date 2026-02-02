drop table tbl1;

create table tbl1 (col string);
alter table tbl1 enable no_drop cascade;
desc extended tbl1;
alter table tbl1 enable no_drop;
desc extended tbl1;
alter table tbl1 disable no_drop cascade;
desc extended tbl1;
alter table tbl1 disable no_drop;

drop table tbl1;

drop table tbl2;
create table tbl2 (col string) partitioned by (p string);
alter table tbl2 add partition (p='p1');
alter table tbl2 add partition (p='p2');
alter table tbl2 add partition (p='p3');
alter table tbl2 enable no_drop cascade;
desc formatted tbl2;
alter table tbl2 disable no_drop cascade;
desc formatted tbl2;
drop table tbl2;
