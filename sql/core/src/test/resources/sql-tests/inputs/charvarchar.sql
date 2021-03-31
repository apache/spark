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

create table char_tbl4(c char(5), v varchar(6)) using parquet;
insert into char_tbl4 values
    (null, null),
    (null, 'E'),
    ('N', null),
    ('Ne', 'Ea'),
    ('Ne  ', 'Ea  '),
    ('Net', 'Ease'),
    ('Net ', 'Ease '),
    ('NetEa ', 'Ease  ');
insert into char_tbl4 values ('NetE  ', 'NetEas');
insert into char_tbl4 values ('NetE  ', 'NetEase');
insert into char_tbl4 values ('NetEase', '');
create table char_tbl5(c char(6)) using parquet;
insert into char_tbl5 select c from char_tbl4;

select c, v from char_tbl4;
select c from char_tbl4 order by c;
select v from char_tbl4 order by v;
select ascii(c), ascii(v) from char_tbl4;
select base64(c), base64(v) from char_tbl4;
select bit_length(t1.c), bit_length(t2.c), bit_length(v) from char_tbl4 t1 join char_tbl5 t2 on t1.c = t2.c;
select char_length(t1.c), char_length(t2.c), char_length(v) from char_tbl4 t1 join char_tbl5 t2 on t1.c = t2.c;
select octet_length(t1.c), octet_length(t2.c), octet_length(v) from char_tbl4 t1 join char_tbl5 t2 on t1.c = t2.c;
select concat_ws('|', t1.c, t2.c), concat_ws('|', t2.c, t1.c) from char_tbl4 t1 join char_tbl5 t2 on t1.c = t2.c;
select concat(t1.c, t2.c), concat(t2.c, t1.c) from char_tbl4 t1 join char_tbl5 t2 on t1.c = t2.c;
select like(c, 'Ne  _') from char_tbl4;
select like(v, 'Ea_') from char_tbl4;
select upper(t1.c), lower(t2.c), upper(t1.c) = upper(t2.c), lower(t1.c) = lower(t2.c) from char_tbl4 t1 join char_tbl5 t2 on t1.c = t2.c;
select printf('Hey, %s%s', c, v) from char_tbl4;
select repect(c, 2), repect(v, 2) from char_tbl4;
select replace(c, 'Ne', 'Ca'), replace(v, 'Ea', 'Ri') from char_tbl4;
select rpad(c, 4), rpad(v, 5) from char_tbl4;
select rpad(c, 5), rpad(v, 6) from char_tbl4;
select rpad(c, 6), rpad(v, 7) from char_tbl4;
select rtrim(c), rtrim(v) from char_tbl4;
select split(c, 'e'), split(v, 'a') from char_tbl4;
select substring(c, 2), substring(v, 3) from char_tbl4;
select left(c, 2), left(v, 3) from char_tbl4;
select right(c, 2), right(v, 3) from char_tbl4;
select typeof(c), typeof(v) from char_tbl4 limit 1;

select cast(NULL as char(1));
select cast('a ' as char(1));
select cast('abcde' as char(1));
select cast('abcde' as char(5));
select cast('abcde' as char(10));

select cast(NULL as varchar(1));
select cast('abcde' as varchar(1));
select cast('abcde' as varchar(5));
select cast('abcde' as varchar(10));

-- char_tbl has renamed to char_tbl1
drop table char_tbl1;
drop table char_tbl2;
drop table char_tbl3;
drop table char_tbl4;
drop table char_tbl5;
