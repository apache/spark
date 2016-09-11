set hive.merge.smallfiles.avgsize=16000000;

DROP TABLE insert_into4a;
DROP TABLE insert_into4b;

CREATE TABLE insert_into4a (key int, value string);
CREATE TABLE insert_into4b (key int, value string);

EXPLAIN INSERT INTO TABLE insert_into4a SELECT * FROM src LIMIT 10;
INSERT INTO TABLE insert_into4a SELECT * FROM src LIMIT 10;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into4a
) t;

EXPLAIN INSERT INTO TABLE insert_into4a SELECT * FROM src LIMIT 10;
INSERT INTO TABLE insert_into4a SELECT * FROM src LIMIT 10;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into4a
) t;

--At this point insert_into4a has 2 files (if INSERT INTO merges isn't fixed)

EXPLAIN INSERT INTO TABLE insert_into4b SELECT * FROM insert_into4a;
INSERT INTO TABLE insert_into4b SELECT * FROM insert_into4a;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into4b
) t;

DROP TABLE insert_into4a;
DROP TABLE insert_into4b;
