create table alter2(a int, b int) partitioned by (insertdate string);
describe extended alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-01') location '2008/01/01';
describe extended alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-02') location '2008/01/02';
describe extended alter2;
show partitions alter2;
drop table alter2;

create external table alter2(a int, b int) partitioned by (insertdate string);
describe extended alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-01') location '2008/01/01';
describe extended alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-02') location '2008/01/02';
describe extended alter2;
show partitions alter2;

-- Cleanup
DROP TABLE alter2;
SHOW TABLES;

-- Using non-default Database

CREATE DATABASE alter2_db;
USE alter2_db;
SHOW TABLES;

CREATE TABLE alter2(a int, b int) PARTITIONED BY (insertdate string);
DESCRIBE EXTENDED alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-01') LOCATION '2008/01/01';
DESCRIBE EXTENDED alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-02') LOCATION '2008/01/02';
DESCRIBE EXTENDED alter2;
SHOW PARTITIONS alter2;
DROP TABLE alter2;

CREATE EXTERNAL TABLE alter2(a int, b int) PARTITIONED BY (insertdate string);
DESCRIBE EXTENDED alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-01') LOCATION '2008/01/01';
DESCRIBE EXTENDED alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-02') LOCATION '2008/01/02';
DESCRIBE EXTENDED alter2;
SHOW PARTITIONS alter2;

DROP TABLE alter2;
USE default;
DROP DATABASE alter2_db;
