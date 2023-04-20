-------------------------------------------------------------------------------------------------
-- Ths testfile tests the ability to templatize identifiers such as table and column names in SQL
-- Common patterns are to to use variable substitution or parameter markers (test in another file)
--------------------------------------------------------------------------------------------------

-- Sanity test variable substitution
SET hivevar:colname = 'c';
SELECT IDENTIFIER(${colname}'_1') FROM VALUES(1) AS T(c_1);

-- Column references
SELECT IDENTIFIER('c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('t.c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('`t`.c1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('c 1') FROM VALUES(1) AS T(`c 1`);
SELECT IDENTIFIER('') FROM VALUES(1) AS T(``);
SELECT IDENTIFIER('c' '1') FROM VALUES(1) AS T(c1);
SELECT IDENTIFIER('t').c1 FROM VALUES(1) AS T(c1);
SELECT T.IDENTIFIER('c1') FROM VALUES(1) AS T(c1);
SELECT T1.c1 FROM VALUES(1) AS T1(c1) JOIN VALUES(1) AS T2(c1) USING (IDENTIFIER('c1'));

-- Map key dereference
SELECT map('a', 1).IDENTIFIER('a') FROM VALUES(1) AS T(c1);

-- Field dereference
SELECT named_struct('a', 1).IDENTIFIER('a') FROM VALUES(1) AS T(c1);

CREATE SCHEMA IF NOT EXISTS s;
CREATE VIEW s.tab(c1) AS VALUES(1);
USE SCHEMA s;

SELECT * FROM IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s.tab');
SELECT * FROM s.IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('`s`.`tab`');
SELECT * FROM IDENTIFIER('s').IDENTIFIER('tab');
SELECT * FROM IDENTIFIER('s').tab;
SELECT * FROM IDENTIFIER('t' 'a' 'b');
SELECT IDENTIFIER('t').* FROM VALUES(1) AS T(c1);

SELECT IDENTIFIER('COALESCE')(NULL, 1);
SELECT row_number() OVER IDENTIFIER('win') FROM VALUES(1) AS T(c1) WINDOW win AS (ORDER BY c1);
SELECT row_number() OVER win FROM VALUES(1) AS T(c1) WINDOW IDENTIFIER('win') AS (ORDER BY c1);

SELECT * FROM VALUES(1),(2),(3) AS T(c1) ORDER BY IDENTIFIER('c1');
SELECT * FROM VALUES(1),(2),(3) AS T(c1) ORDER BY IDENTIFIER('c1') DESC;

with identifier('v')(identifier('c1')) AS (VALUES(1)) (SELECT c1 FROM v);

USE SCHEMA default;
DROP VIEW s.tab;
DROP SCHEMA s;

SELECT IDENTIFIER('abs')(-1);

USE IDENTIFIER('default');

CREATE TABLE IDENTIFIER('tab')(c1 INT) USING parquet;
DROP TABLE IF EXISTS IDENTIFIER('tab');

CREATE TABLE tab(IDENTIFIER('c1') INT) USING parquet;
INSERT INTO tab VALUES (1);
INSERT INTO IDENTIFIER('tab') VALUES(1);
INSERT INTO tab(IDENTIFIER('c1')) VALUES(1);
SELECT c1 FROM tab;
DESCRIBE IDENTIFIER('tab');
DROP TABLE IF EXISTS tab;

CREATE OR REPLACE VIEW IDENTIFIER('v')(c1) AS VALUES(1);
SELECT * FROM v;
DROP VIEW IDENTIFIER('v');

CREATE TEMPORARY VIEW IDENTIFIER('v')(c1) AS VALUES(1);
SELECT * FROM v;
DROP VIEW IDENTIFIER('v');

CREATE OR REPLACE VIEW v(IDENTIFIER('c1')) AS VALUES(1);
SELECT c1 FROM v;
DROP VIEW v;

-- Error conditions
SELECT row_number() OVER IDENTIFIER('x.win') FROM VALUES(1) AS T(c1) WINDOW win AS (ORDER BY c1);