create table if not exists alltypes (
 bo boolean,
 ti tinyint,
 si smallint,
 i int,
 bi bigint,
 f float,
 d double,
 de decimal(10,3),
 ts timestamp,
 da date,
 s string,
 c char(5),
 vc varchar(5),
 m map<string, string>,
 l array<int>,
 st struct<c1:int, c2:string>
) row format delimited fields terminated by '|'
collection items terminated by ','
map keys terminated by ':' stored as textfile;

create table alltypes_orc like alltypes;
alter table alltypes_orc set fileformat orc;

load data local inpath '../../data/files/alltypes2.txt' overwrite into table alltypes;

insert overwrite table alltypes_orc select * from alltypes;

select min(bo), max(bo), min(ti), max(ti), min(si), max(si), min(i), max(i), min(bi), max(bi), min(f), max(f), min(d), max(d), min(de), max(de), min(ts), max(ts), min(da), max(da), min(s), max(s), min(c), max(c), min(vc), max(vc) from alltypes;

select min(bo), max(bo), min(ti), max(ti), min(si), max(si), min(i), max(i), min(bi), max(bi), min(f), max(f), min(d), max(d), min(de), max(de), min(ts), max(ts), min(da), max(da), min(s), max(s), min(c), max(c), min(vc), max(vc) from alltypes_orc;

