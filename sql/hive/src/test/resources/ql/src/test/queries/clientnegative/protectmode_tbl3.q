-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode_4;

create table tbl_protectmode_4  (col string);
select col from tbl_protectmode_4;
alter table tbl_protectmode_4 enable offline;
desc extended tbl_protectmode_4;

select col from tbl_protectmode_4;
