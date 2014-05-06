

create table insert1(key int, value string) stored as textfile;
create table insert2(key int, value string) stored as textfile;
insert overwrite table insert1 select a.key, a.value from insert2 a WHERE (a.key=-1);

explain insert into table insert1 select a.key, a.value from insert2 a WHERE (a.key=-1);
explain insert into table INSERT1 select a.key, a.value from insert2 a WHERE (a.key=-1);

-- HIVE-3465
create database x;
create table x.insert1(key int, value string) stored as textfile;

explain insert into table x.INSERT1 select a.key, a.value from insert2 a WHERE (a.key=-1);

explain insert into table default.INSERT1 select a.key, a.value from insert2 a WHERE (a.key=-1);

explain
from insert2
insert into table insert1 select * where key < 10
insert overwrite table x.insert1 select * where key > 10 and key < 20;

-- HIVE-3676
CREATE DATABASE db2;
USE db2;
CREATE TABLE result(col1 STRING);
INSERT OVERWRITE TABLE result SELECT 'db2_insert1' FROM default.src LIMIT 1;
INSERT INTO TABLE result SELECT 'db2_insert2' FROM default.src LIMIT 1;
SELECT * FROM result order by col1;

USE default;
CREATE DATABASE db1;
CREATE TABLE db1.result(col1 STRING);
INSERT OVERWRITE TABLE db1.result SELECT 'db1_insert1' FROM src LIMIT 1;
INSERT INTO TABLE db1.result SELECT 'db1_insert2' FROM src LIMIT 1;
SELECT * FROM db1.result order by col1;
