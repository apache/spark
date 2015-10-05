DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
DROP TABLE IF EXISTS table3;
DROP VIEW IF EXISTS view1;

CREATE TABLE table1 (a STRING, b STRING) STORED AS TEXTFILE;
DESCRIBE table1;
DESCRIBE FORMATTED table1;

CREATE VIEW view1 AS SELECT * FROM table1;

CREATE TABLE table2 LIKE view1;
DESCRIBE table2;
DESCRIBE FORMATTED table2;

CREATE TABLE IF NOT EXISTS table2 LIKE view1;

CREATE EXTERNAL TABLE IF NOT EXISTS table2 LIKE view1;

CREATE EXTERNAL TABLE IF NOT EXISTS table3 LIKE view1;
DESCRIBE table3;
DESCRIBE FORMATTED table3;

INSERT OVERWRITE TABLE table1 SELECT key, value FROM src WHERE key = 86;
INSERT OVERWRITE TABLE table2 SELECT key, value FROM src WHERE key = 100;

SELECT * FROM table1 order by a, b;
SELECT * FROM table2 order by a, b;

DROP TABLE table1;
DROP TABLE table2;
DROP VIEW view1;

-- check partitions
create view view1 partitioned on (ds, hr) as select * from srcpart;
create table table1 like view1;
describe formatted table1;
DROP TABLE table1;
DROP VIEW view1;