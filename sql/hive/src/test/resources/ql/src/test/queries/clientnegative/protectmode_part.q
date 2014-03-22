-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode3;

create table tbl_protectmode3  (col string) partitioned by (p string);
alter table tbl_protectmode3 add partition (p='p1');
alter table tbl_protectmode3 add partition (p='p2');

select * from tbl_protectmode3 where p='p1';
select * from tbl_protectmode3 where p='p2';

alter table tbl_protectmode3 partition (p='p1') enable offline;

select * from tbl_protectmode3 where p='p2';
select * from tbl_protectmode3 where p='p1';
