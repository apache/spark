DROP TABLE insert_into3a;
DROP TABLE insert_into3b;

CREATE TABLE insert_into3a (key int, value string);
CREATE TABLE insert_into3b (key int, value string);

EXPLAIN FROM src INSERT INTO TABLE insert_into3a SELECT * LIMIT 50
                 INSERT INTO TABLE insert_into3b SELECT * LIMIT 100;
FROM src INSERT INTO TABLE insert_into3a SELECT * LIMIT 50
         INSERT INTO TABLE insert_into3b SELECT * LIMIT 100;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into3a
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into3b
) t;

EXPLAIN FROM src INSERT OVERWRITE TABLE insert_into3a SELECT * LIMIT 10
                 INSERT INTO TABLE insert_into3b SELECT * LIMIT 10;
FROM src INSERT OVERWRITE TABLE insert_into3a SELECT * LIMIT 10
         INSERT INTO TABLE insert_into3b SELECT * LIMIT 10;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into3a
) t;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into3b
) t;

DROP TABLE insert_into3a;
DROP TABLE insert_into3b;
