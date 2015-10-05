-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_tbl4;
drop table tbl_protectmode_tbl4_src;

create table tbl_protectmode_tbl4_src (col string);

create table tbl_protectmode_tbl4  (col string) partitioned by (p string);
alter table tbl_protectmode_tbl4 add partition (p='p1');
alter table tbl_protectmode_tbl4 enable no_drop;
alter table tbl_protectmode_tbl4 enable offline;
alter table tbl_protectmode_tbl4 disable no_drop;
desc extended tbl_protectmode_tbl4;

select col from tbl_protectmode_tbl4 where p='not_exist';
