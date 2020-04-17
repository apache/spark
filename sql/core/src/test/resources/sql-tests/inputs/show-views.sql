-- Test data.
CREATE DATABASE showdb;
USE showdb;
CREATE TABLE tbl(a STRING, b INT, c STRING, d STRING) USING parquet;
CREATE VIEW view_1 AS SELECT * FROM tbl;
CREATE VIEW view_2 AS SELECT * FROM tbl WHERE c='a';
CREATE GLOBAL TEMP VIEW view_3 AS SELECT 1 as col1;
CREATE TEMPORARY VIEW view_4(e INT) USING parquet;

-- SHOW VIEWS
SHOW VIEWS;
SHOW VIEWS FROM showdb;
SHOW VIEWS IN showdb;
SHOW VIEWS IN global_temp;

-- SHOW VIEWS WITH wildcard match
SHOW VIEWS 'view_*';
SHOW VIEWS LIKE 'view_1*|view_2*';
SHOW VIEWS IN showdb 'view_*';
SHOW VIEWS IN showdb LIKE 'view_*';
-- Error when database not exists
SHOW VIEWS IN wrongdb LIKE 'view_*';

-- Clean Up
DROP VIEW global_temp.view_3;
DROP VIEW view_4;
USE default;
DROP DATABASE showdb CASCADE;
