-- protect mode: syntax to change protect mode works and queries are not blocked if a table or partition is not in protect mode

drop table tbl_protectmode6;

create table tbl_protectmode6  (c1 string,c2 string) partitioned by (p string);
alter table tbl_protectmode6 add partition (p='p1');
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' OVERWRITE INTO TABLE tbl_protectmode6 partition (p='p1');
alter table tbl_protectmode6 partition (p='p1') enable offline; 
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' OVERWRITE INTO TABLE tbl_protectmode6 partition (p='p1');
