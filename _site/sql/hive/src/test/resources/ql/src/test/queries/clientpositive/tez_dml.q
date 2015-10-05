set hive.exec.dynamic.partition.mode=nonstrict;

-- CTAS
EXPLAIN CREATE TABLE tmp_src AS SELECT * FROM (SELECT value, count(value) AS cnt FROM src GROUP BY value) f1 ORDER BY cnt;
CREATE TABLE tmp_src AS SELECT * FROM (SELECT value, count(value) AS cnt FROM src GROUP BY value) f1 ORDER BY cnt;

SELECT * FROM tmp_src;

-- dyn partitions
CREATE TABLE tmp_src_part (c string) PARTITIONED BY (d int);
EXPLAIN INSERT INTO TABLE tmp_src_part PARTITION (d) SELECT * FROM tmp_src;
INSERT INTO TABLE tmp_src_part PARTITION (d) SELECT * FROM tmp_src;

SELECT * FROM tmp_src_part;

-- multi insert
CREATE TABLE even (c int, d string);
CREATE TABLE odd (c int, d string);

EXPLAIN
FROM src
INSERT INTO TABLE even SELECT key, value WHERE key % 2 = 0 
INSERT INTO TABLE odd SELECT key, value WHERE key % 2 = 1;

FROM src
INSERT INTO TABLE even SELECT key, value WHERE key % 2 = 0 
INSERT INTO TABLE odd SELECT key, value WHERE key % 2 = 1;

SELECT * FROM even;
SELECT * FROM odd;

-- create empty table
CREATE TABLE empty STORED AS orc AS SELECT * FROM tmp_src_part WHERE d = -1000;
SELECT * FROM empty;

-- drop the tables
DROP TABLE even;
DROP TABLE odd;
DROP TABLE tmp_src;
DROP TABLE tmp_src_part;
