-- create table with 1000 rows
create table srcorc(key string, value string) stored as textfile;
insert overwrite table srcorc select * from src;
insert into table srcorc select * from src;

-- load table with each row group having 1000 rows and stripe 1 & 2 having 5000 & 2000 rows respectively
create table if not exists vectororc
(s1 string,
s2 string,
d double,
s3 string)
stored as ORC tblproperties("orc.row.index.stride"="1000", "orc.stripe.size"="100000", "orc.compress.size"="10000");

-- insert creates separate orc files
insert overwrite table vectororc select "apple", "a", rand(1), "zoo" from srcorc;
insert into table vectororc select null, "b", rand(2), "zoo" from srcorc;
insert into table vectororc select null, "c", rand(3), "zoo" from srcorc;
insert into table vectororc select "apple", "d", rand(4), "zoo" from srcorc;
insert into table vectororc select null, "e", rand(5), "z" from srcorc;
insert into table vectororc select "apple", "f", rand(6), "z" from srcorc;
insert into table vectororc select null, "g", rand(7), "zoo" from srcorc;

-- since vectororc table has multiple orc file we will load them into a single file using another table
create table if not exists testorc
(s1 string,
s2 string,
d double,
s3 string)
stored as ORC tblproperties("orc.row.index.stride"="1000", "orc.stripe.size"="100000", "orc.compress.size"="10000");
insert overwrite table testorc select * from vectororc order by s2;

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.optimize.index.filter=true;

set hive.vectorized.execution.enabled=false;
-- row group (1,4) from stripe 1 and row group (1) from stripe 2
-- PPD ONLY
select count(*),int(sum(d)) from testorc where s1 is not null;
set hive.vectorized.execution.enabled=true;
-- VECTORIZATION + PPD
select count(*),int(sum(d)) from testorc where s1 is not null;

set hive.vectorized.execution.enabled=false;
-- row group (2,3,5) from stripe 1 and row group (2) from stripe 2
-- PPD ONLY
select count(*),int(sum(d)) from testorc where s2 in ("b", "c", "e", "g");
set hive.vectorized.execution.enabled=true;
-- VECTORIZATION + PPD
select count(*),int(sum(d)) from testorc where s2 in ("b", "c", "e", "g");

set hive.vectorized.execution.enabled=false;
-- last row group of stripe 1 and first row group of stripe 2
-- PPD ONLY
select count(*),int(sum(d)) from testorc where s3="z";
set hive.vectorized.execution.enabled=true;
-- VECTORIZATION + PPD
select count(*),int(sum(d)) from testorc where s3="z";

set hive.vectorized.execution.enabled=false;
-- first row group of stripe 1 and last row group of stripe 2
-- PPD ONLY
select count(*),int(sum(d)) from testorc where s2="a" or s2="g";
set hive.vectorized.execution.enabled=true;
-- VECTORIZATION + PPD
select count(*),int(sum(d)) from testorc where s2="a" or s2="g";

drop table srcorc;
drop table vectororc;
drop table testorc;
