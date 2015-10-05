-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode5;

create table tbl_protectmode5_1 (col string);

create table tbl_protectmode5  (col string) partitioned by (p string);
alter table tbl_protectmode5 add partition (p='p1');
alter table tbl_protectmode5 add partition (p='p2');

insert overwrite table tbl_protectmode5_1
select col from tbl_protectmode5 where p='p1';
insert overwrite table tbl_protectmode5_1
select col from tbl_protectmode5 where p='p2';

alter table tbl_protectmode5 partition (p='p1') enable offline;

insert overwrite table tbl_protectmode5_1
select col from tbl_protectmode5 where p='p2';
insert overwrite table tbl_protectmode5_1
select col from tbl_protectmode5 where p='p1';
