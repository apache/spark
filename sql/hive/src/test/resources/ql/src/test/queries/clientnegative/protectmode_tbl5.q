-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_tbl5;
drop table tbl_protectmode_tbl5_src;

create table tbl_protectmode_tbl5_src (col string);

create table tbl_protectmode_tbl5  (col string) partitioned by (p string);
alter table tbl_protectmode_tbl5 add partition (p='p1');
alter table tbl_protectmode_tbl5 enable no_drop;
alter table tbl_protectmode_tbl5 enable offline;
alter table tbl_protectmode_tbl5 disable no_drop;
desc extended tbl_protectmode_tbl5;

insert overwrite table tbl_protectmode_tbl5 partition (p='not_exist') select col from tbl_protectmode_tbl5_src;
