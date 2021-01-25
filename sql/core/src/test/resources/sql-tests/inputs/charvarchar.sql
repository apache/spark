create table char_tbl(c char(5), v varchar(6)) using parquet;
desc formatted char_tbl;
desc formatted char_tbl c;
show create table char_tbl;

create table char_tbl2 using parquet as select * from char_tbl;
show create table char_tbl2;
desc formatted char_tbl2;
desc formatted char_tbl2 c;

create table char_tbl3 like char_tbl;
desc formatted char_tbl3;
desc formatted char_tbl3 c;
show create table char_tbl3;

create view char_view as select * from char_tbl;
desc formatted char_view;
desc formatted char_view c;
show create table char_view;

alter table char_tbl rename to char_tbl1;
desc formatted char_tbl1;

alter table char_tbl1 change column c type char(6);
alter table char_tbl1 change column c type char(5);
desc formatted char_tbl1;

alter table char_tbl1 add columns (d char(5));
desc formatted char_tbl1;

alter view char_view as select * from char_tbl2;
desc formatted char_view;

alter table char_tbl1 SET TBLPROPERTIES('yes'='no');
desc formatted char_tbl1;

alter view char_view SET TBLPROPERTIES('yes'='no');
desc formatted char_view;

alter table char_tbl1 UNSET TBLPROPERTIES('yes');
desc formatted char_tbl1;

alter view char_view UNSET TBLPROPERTIES('yes');
desc formatted char_view;

alter table char_tbl1 SET SERDEPROPERTIES('yes'='no');
desc formatted char_tbl1;

create table char_part(c1 char(5), c2 char(2), v1 varchar(6), v2 varchar(2)) using parquet partitioned by (v2, c2);
desc formatted char_part;

alter table char_part add partition (v2='ke', c2='nt') location 'loc1';
desc formatted char_part;

alter table char_part partition (v2='ke') rename to partition (v2='nt');
desc formatted char_part;

alter table char_part partition (v2='ke', c2='nt') set location 'loc2';
desc formatted char_part;

MSCK REPAIR TABLE char_part;
desc formatted char_part;

-- char_tbl has renamed to char_tbl1
drop table char_tbl1;
drop table char_tbl2;
drop table char_tbl3;
