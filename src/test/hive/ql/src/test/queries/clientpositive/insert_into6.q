set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

DROP TABLE insert_into6a;
DROP TABLE insert_into6b;
CREATE TABLE insert_into6a (key int, value string) PARTITIONED BY (ds string);
CREATE TABLE insert_into6b (key int, value string) PARTITIONED BY (ds string);

EXPLAIN INSERT INTO TABLE insert_into6a PARTITION (ds='1') 
    SELECT * FROM src LIMIT 150;
INSERT INTO TABLE insert_into6a PARTITION (ds='1') SELECT * FROM src LIMIT 150;
INSERT INTO TABLE insert_into6a PARTITION (ds='2') SELECT * FROM src LIMIT 100;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into6a
) t;

EXPLAIN INSERT INTO TABLE insert_into6b PARTITION (ds) 
    SELECT * FROM insert_into6a;
INSERT INTO TABLE insert_into6b PARTITION (ds) SELECT * FROM insert_into6a;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into6b
) t;

SHOW PARTITIONS insert_into6b;

DROP TABLE insert_into6a;
DROP TABLE insert_into6b;

