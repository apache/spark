-- Test data.
CREATE NAMESPACE IF NOT EXISTS query_ddl_namespace;
USE NAMESPACE query_ddl_namespace;
CREATE TABLE test_show_tables(a INT, b STRING, c INT) using parquet;
CREATE TABLE test_show_table_properties (a INT, b STRING, c INT) USING parquet TBLPROPERTIES('p1'='v1', 'p2'='v2');
CREATE TABLE test_show_partitions(a String, b Int, c String, d String) USING parquet PARTITIONED BY (c, d);
ALTER TABLE test_show_partitions ADD PARTITION (c='Us', d=1);
ALTER TABLE test_show_partitions ADD PARTITION (c='Us', d=2);
ALTER TABLE test_show_partitions ADD PARTITION (c='Cn', d=1);
CREATE VIEW view_1 AS SELECT * FROM test_show_tables;
CREATE VIEW view_2 AS SELECT * FROM test_show_tables WHERE c=1;
CREATE TEMPORARY VIEW test_show_views(e int) USING parquet;
CREATE GLOBAL TEMP VIEW test_global_show_views AS SELECT 1 as col1;

-- SHOW NAMESPACES
SHOW NAMESPACES;
WITH s AS (SHOW NAMESPACES) SELECT * FROM s;
WITH s AS (SHOW NAMESPACES) SELECT * FROM s WHERE namespace = 'query_ddl_namespace';
WITH s(n) AS (SHOW NAMESPACES) SELECT * FROM s WHERE n = 'query_ddl_namespace';

-- SHOW TABLES
SHOW TABLES;
WITH s AS (SHOW TABLES) SELECT * FROM s;
WITH s AS (SHOW TABLES) SELECT * FROM s WHERE tableName = 'test_show_tables';
WITH s(ns, tn, t) AS (SHOW TABLES) SELECT * FROM s WHERE tn = 'test_show_tables';

-- SHOW TBLPROPERTIES
SHOW TBLPROPERTIES test_show_table_properties;
WITH s AS (SHOW TBLPROPERTIES test_show_table_properties) SELECT * FROM s;
WITH s AS (SHOW TBLPROPERTIES test_show_table_properties) SELECT * FROM s WHERE key = 'p1';
WITH s(k, v) AS (SHOW TBLPROPERTIES test_show_table_properties) SELECT * FROM s WHERE k = 'p1';

-- SHOW PARTITIONS
SHOW PARTITIONS test_show_partitions;
WITH s AS (SHOW PARTITIONS test_show_partitions) SELECT * FROM s;
WITH s AS (SHOW PARTITIONS test_show_partitions) SELECT * FROM s WHERE partition = 'c=Us/d=1';
WITH s(p) AS (SHOW PARTITIONS test_show_partitions) SELECT * FROM s WHERE p = 'c=Us/d=1';

-- SHOW COLUMNS
SHOW COLUMNS in test_show_tables;
WITH s AS (SHOW COLUMNS in test_show_tables) SELECT * FROM s;
WITH s AS (SHOW COLUMNS in test_show_tables) SELECT * FROM s WHERE col_name = 'a';
WITH s(c) AS (SHOW COLUMNS in test_show_tables) SELECT * FROM s WHERE c = 'a';

-- SHOW VIEWS
SHOW VIEWS;
WITH s AS (SHOW VIEWS) SELECT * FROM s;
WITH s AS (SHOW VIEWS) SELECT * FROM s WHERE viewName = 'test_show_views';
WITH s(ns, vn, t) AS (SHOW VIEWS) SELECT * FROM s WHERE vn = 'test_show_views';

-- SHOW FUNCTIONS
WITH s AS (SHOW FUNCTIONS) SELECT * FROM s LIMIT 3;
WITH s AS (SHOW FUNCTIONS) SELECT * FROM s WHERE function LIKE 'an%';
WITH s(f) AS (SHOW FUNCTIONS) SELECT * FROM s WHERE f LIKE 'an%';

-- Clean Up
DROP VIEW global_temp.test_global_show_views;
DROP VIEW test_show_views;
DROP VIEW view_2;
DROP VIEW view_1;
DROP TABLE test_show_partitions;
DROP TABLE test_show_table_properties;
DROP TABLE test_show_tables;
USE default;
DROP NAMESPACE query_ddl_namespace;