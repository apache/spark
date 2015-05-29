-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl1;
drop table tbl2;

create table tbl1  (col string);
select * from tbl1;
select col from tbl1;
alter table tbl1 enable offline;
desc extended tbl1;
alter table tbl1 disable offline;
desc extended tbl1;
select * from tbl1;
select col from tbl1;
 
create table tbl2  (col string) partitioned by (p string);
alter table tbl2 add partition (p='p1');
alter table tbl2 add partition (p='p2');
alter table tbl2 add partition (p='p3');
alter table tbl2 drop partition (p='not_exist');

select * from tbl2 where p='p1';
select * from tbl2 where p='p2';

alter table tbl2 partition (p='p1') enable offline;
desc extended tbl2 partition (p='p1');

alter table tbl2 enable offline;
desc extended tbl2;

alter table tbl2 enable no_drop;
desc extended tbl2;
alter table tbl2 drop partition (p='p3');

alter table tbl2 disable offline;
desc extended tbl2;

alter table tbl2 disable no_drop;
desc extended tbl2;

select * from tbl2 where p='p2';
select col from tbl2 where p='p2';

alter table tbl2 partition (p='p1') disable offline;
desc extended tbl2 partition (p='p1');

select * from tbl2 where p='p1';
select col from tbl2 where p='p1';

insert overwrite table tbl1 select col from tbl2 where p='p1';
insert overwrite table tbl1 select col from tbl1;

alter table tbl2 partition (p='p1') enable no_drop;
alter table tbl2 partition (p='p1') disable no_drop;

alter table tbl2 partition (p='p2') enable no_drop;

alter table tbl2 drop partition (p='p1');

alter table tbl2 partition (p='p2') disable no_drop;

drop table tbl1;
drop table tbl2;