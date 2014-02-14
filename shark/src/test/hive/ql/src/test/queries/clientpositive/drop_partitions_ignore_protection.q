create table tbl_protectmode_no_drop  (c1 string,c2 string) partitioned by (p string);
alter table tbl_protectmode_no_drop add partition (p='p1');
alter table tbl_protectmode_no_drop partition (p='p1') enable no_drop;
desc extended tbl_protectmode_no_drop partition (p='p1');

-- The partition will be dropped, even though we have enabled no_drop
-- as 'ignore protection' has been specified in the command predicate
alter table tbl_protectmode_no_drop drop partition (p='p1') ignore protection;
drop table tbl_protectmode_no_drop;

