-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode2;

create table tbl_protectmode2  (col string) partitioned by (p string);
alter table tbl_protectmode2 add partition (p='p1');
alter table tbl_protectmode2 enable no_drop;
alter table tbl_protectmode2 enable offline;
alter table tbl_protectmode2 disable no_drop;
desc extended tbl_protectmode2;

select * from tbl_protectmode2 where p='p1';
