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

