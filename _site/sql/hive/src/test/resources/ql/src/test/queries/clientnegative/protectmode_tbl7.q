-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_tbl7;
create table tbl_protectmode_tbl7  (col string) partitioned by (p string);
alter table tbl_protectmode_tbl7 add partition (p='p1');
alter table tbl_protectmode_tbl7 enable no_drop;

alter table tbl_protectmode_tbl7 drop partition (p='p1');

alter table tbl_protectmode_tbl7 add partition (p='p1');
alter table tbl_protectmode_tbl7 enable no_drop cascade;

alter table tbl_protectmode_tbl7 drop partition (p='p1');
