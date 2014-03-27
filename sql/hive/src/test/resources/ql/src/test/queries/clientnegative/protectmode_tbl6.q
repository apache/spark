-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_tbl6;

create table tbl_protectmode_tbl6 (col string);
alter table tbl_protectmode_tbl6 enable no_drop cascade;

drop table tbl_protectmode_tbl6;
