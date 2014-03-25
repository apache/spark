set hive.limit.optimize.enable=true;
set hive.limit.optimize.limit.file=2;

drop table gl_tgt;
drop table gl_src1;
drop table gl_src2;
drop table gl_src_part1;


create table gl_src1 (key int, value string) stored as textfile;
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE gl_src1;
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE gl_src1;
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE gl_src1;




set hive.limit.row.max.size=100;

-- need one file
create table gl_tgt as select key from gl_src1 limit 1;
select * from gl_tgt ORDER BY key ASC;
-- need two files
select 'x' as key_new , split(value,',') as value_new from gl_src1 ORDER BY key_new ASC, value_new[0] ASC limit 20;
-- no sufficient files
select key, value, split(value,',') as value_new from gl_src1 ORDER BY key ASC, value ASC, value_new[0] ASC limit 30;
-- need all files
select key from gl_src1 ORDER BY key ASC limit 100;
set hive.limit.optimize.limit.file=4;
select key from gl_src1 ORDER BY key ASC limit 30;

-- not qualified cases
select key, count(1) from gl_src1 group by key ORDER BY key ASC limit 5;
select distinct key from gl_src1 ORDER BY key ASC limit 10;
select count(1) from gl_src1 limit 1;
select transform(*) using "tr _ \n" as t from
(select "a_a_a_a_a_a_" from gl_src1 limit 100) subq ORDER BY t;
select key from (select * from (select key,value from gl_src1)t1 limit 10)t2 ORDER BY key ASC limit 2000;

-- complicated queries
select key from (select * from (select key,value from gl_src1 limit 10)t1 )t2 ORDER BY key ASC;
select key from (select * from (select key,value from gl_src1)t1 limit 10)t2 ORDER BY key ASC;
insert overwrite table gl_tgt select key+1 from (select * from (select key,value from gl_src1)t1)t2 limit 10;
select * from gl_tgt ORDER BY key ASC;

-- empty table
create table gl_src2 (key int, value string) stored as textfile;
select key from gl_src2 ORDER BY key ASC limit 10;

-- partition
create table gl_src_part1 (key int, value string) partitioned by (p string) stored as textfile;
load data local inpath '../data/files/srcbucket21.txt' INTO TABLE gl_src_part1 partition(p='11');
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE gl_src_part1 partition(p='12');
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE gl_src_part1 partition(p='12');
load data local inpath '../data/files/srcbucket20.txt' INTO TABLE gl_src_part1 partition(p='12');

select key from gl_src_part1 where p like '1%' ORDER BY key ASC limit 10;
select key from gl_src_part1 where p='11' ORDER BY key ASC limit 10;
select key from gl_src_part1 where p='12' ORDER BY key ASC limit 10;
select key from gl_src_part1 where p='13' ORDER BY key ASC limit 10;
alter table gl_src_part1 add partition (p='13');
select key from gl_src_part1 where p='13' ORDER BY key ASC limit 10;
select key from gl_src_part1 where p='12' ORDER BY key ASC limit 1000;

drop table gl_src1;
drop table gl_src2;
drop table gl_src_part1;
drop table gl_tgt;
