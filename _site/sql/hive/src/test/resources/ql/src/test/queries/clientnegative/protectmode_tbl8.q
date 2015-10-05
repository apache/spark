-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_tbl8;
create table tbl_protectmode_tbl8  (col string) partitioned by (p string);
alter table tbl_protectmode_tbl8 add partition (p='p1');
alter table tbl_protectmode_tbl8 enable no_drop;

alter table tbl_protectmode_tbl8 drop partition (p='p1');

alter table tbl_protectmode_tbl8 enable no_drop cascade;

alter table tbl_protectmode_tbl8 add partition (p='p1');
alter table tbl_protectmode_tbl8 drop partition (p='p1');
