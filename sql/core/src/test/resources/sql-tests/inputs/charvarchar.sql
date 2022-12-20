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

create temporary view str_view as select c, v from values
    (null, null),
    (null, 'S'),
    ('N', 'N '),
    ('Ne', 'Sp'),
    ('Net  ', 'Spa  '),
    ('NetE', 'Spar'),
    ('NetEa ', 'Spark '),
    ('NetEas ', 'Spark'),
    ('NetEase', 'Spark-') t(c, v);

create table char_tbl4(c7 char(7), c8 char(8), v varchar(6), s string) using parquet;
insert into char_tbl4 select c, c, v, c from str_view;

select c7, c8, v, s from char_tbl4;
select c7, c8, v, s from char_tbl4 where c7 = c8;
select c7, c8, v, s from char_tbl4 where c7 = v;
select c7, c8, v, s from char_tbl4 where c7 = s;
select c7, c8, v, s from char_tbl4 where c7 = 'NetEase               ';
select c7, c8, v, s from char_tbl4 where v = 'Spark ';
select c7, c8, v, s from char_tbl4 order by c7;
select c7, c8, v, s from char_tbl4 order by v;

select ascii(c7), ascii(c8), ascii(v), ascii(s) from char_tbl4;
select base64(c7), base64(c8), base64(v), ascii(s) from char_tbl4;
select bit_length(c7), bit_length(c8), bit_length(v), bit_length(s) from char_tbl4;
select char_length(c7), char_length(c8), char_length(v), char_length(s) from char_tbl4;
select octet_length(c7), octet_length(c8), octet_length(v), octet_length(s) from char_tbl4;
select concat_ws('|', c7, c8), concat_ws('|', c7, v), concat_ws('|', c7, s), concat_ws('|', v, s) from char_tbl4;
select concat(c7, c8), concat(c7, v), concat(c7, s), concat(v, s) from char_tbl4;
select like(c7, 'Ne     _'), like(c8, 'Ne     _') from char_tbl4;
select like(v, 'Spark_') from char_tbl4;
select c7 = c8, upper(c7) = upper(c8), lower(c7) = lower(c8) from char_tbl4 where s = 'NetEase';
select c7 = s, upper(c7) = upper(s), lower(c7) = lower(s) from char_tbl4 where s = 'NetEase';
select c7 = 'NetEase', upper(c7) = upper('NetEase'), lower(c7) = lower('NetEase') from char_tbl4 where s = 'NetEase';
select printf('Hey, %s%s%s%s', c7, c8, v, s) from char_tbl4;
select repeat(c7, 2), repeat(c8, 2), repeat(v, 2), repeat(s, 2) from char_tbl4;
select replace(c7, 'Net', 'Apache'), replace(c8, 'Net', 'Apache'), replace(v, 'Spark', 'Kyuubi'), replace(s, 'Net', 'Apache') from char_tbl4;
select rpad(c7, 10), rpad(c8, 5), rpad(v, 5), rpad(s, 5)  from char_tbl4;
select rtrim(c7), rtrim(c8), rtrim(v), rtrim(s) from char_tbl4;
select split(c7, 'e'), split(c8, 'e'), split(v, 'a'), split(s, 'e') from char_tbl4;
select substring(c7, 2), substring(c8, 2), substring(v, 3), substring(s, 2) from char_tbl4;
select left(c7, 2), left(c8, 2), left(v, 3), left(s, 2) from char_tbl4;
select right(c7, 2), right(c8, 2), right(v, 3), right(s, 2) from char_tbl4;
select typeof(c7), typeof(c8), typeof(v), typeof(s) from char_tbl4 limit 1;
select cast(c7 as char(1)), cast(c8 as char(10)), cast(v as char(1)), cast(v as varchar(1)), cast(s as char(5)) from char_tbl4;

-- char_tbl has renamed to char_tbl1
drop table char_tbl1;
drop table char_tbl2;
drop table char_tbl3;
drop table char_tbl4;

-- ascii value for Latin-1 Supplement characters
select ascii('§'), ascii('÷'), ascii('×10');
select chr(167), chr(247), chr(215);
