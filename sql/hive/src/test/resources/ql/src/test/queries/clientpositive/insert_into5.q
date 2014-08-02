DROP TABLE insert_into5a;
DROP TABLE insert_into5b;

CREATE TABLE insert_into5a (key int, value string);
CREATE TABLE insert_into5b (key int, value string) PARTITIONED BY (ds string);

EXPLAIN INSERT INTO TABLE insert_into5a SELECT 1, 'one' FROM src LIMIT 10;
INSERT INTO TABLE insert_into5a SELECT 1, 'one' FROM src LIMIT 10;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into5a
) t;

EXPLAIN INSERT INTO TABLE insert_into5a SELECT * FROM insert_into5a;
INSERT INTO TABLE insert_into5a SELECT * FROM insert_into5a;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into5a
) t;

EXPLAIN INSERT INTO TABLE insert_into5b PARTITION (ds='1') 
  SELECT * FROM insert_into5a;
INSERT INTO TABLE insert_into5b PARTITION (ds='1') SELECT * FROM insert_into5a;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into5b
) t;

EXPLAIN INSERT INTO TABLE insert_into5b PARTITION (ds='1')
  SELECT key, value FROM insert_into5b;
INSERT INTO TABLE insert_into5b PARTITION (ds='1') 
  SELECT key, value FROM insert_into5b;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into5b
) t;

DROP TABLE insert_into5a;
