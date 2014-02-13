DROP TABLE insert_into2;
CREATE TABLE insert_into2 (key int, value string) 
  PARTITIONED BY (ds string);

EXPLAIN INSERT INTO TABLE insert_into2 PARTITION (ds='1') 
  SELECT * FROM src LIMIT 100;
INSERT INTO TABLE insert_into2 PARTITION (ds='1') SELECT * FROM src limit 100;
INSERT INTO TABLE insert_into2 PARTITION (ds='1') SELECT * FROM src limit 100;
SELECT COUNT(*) FROM insert_into2 WHERE ds='1';
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;

EXPLAIN INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 100;
INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 100;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;

EXPLAIN INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 50;
INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 50;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;

DROP TABLE insert_into2;
