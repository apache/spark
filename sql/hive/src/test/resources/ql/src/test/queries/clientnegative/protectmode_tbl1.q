-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_1;

create table tbl_protectmode_1  (col string);
select * from tbl_protectmode_1;
alter table tbl_protectmode_1 enable offline;
select * from tbl_protectmode_1;
