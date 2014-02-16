-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode__no_drop;

create table tbl_protectmode__no_drop  (col string);
select * from tbl_protectmode__no_drop;
alter table tbl_protectmode__no_drop enable no_drop;
desc extended tbl_protectmode__no_drop;
drop table tbl_protectmode__no_drop;
