-- Test data.
CREATE NAMESPACE IF NOT EXISTS table_valued_command;
USE NAMESPACE table_valued_command;
CREATE TABLE test_show_tables(a INT, b STRING, c INT) using parquet;
CREATE TABLE test_show_table_properties (a INT, b STRING, c INT) USING parquet TBLPROPERTIES('p1'='v1', 'p2'='v2');
CREATE TABLE test_show_partitions(a String, b Int, c String, d String) USING parquet PARTITIONED BY (c, d);
ALTER TABLE test_show_partitions ADD PARTITION (c='Us', d=1);
ALTER TABLE test_show_partitions ADD PARTITION (c='Us', d=2);
ALTER TABLE test_show_partitions ADD PARTITION (c='Cn', d=1);
-- CREATE TEMPORARY VIEW test_show_views(e int) USING parquet;
-- CREATE GLOBAL TEMP VIEW test_global_show_views AS SELECT 1 as col1;

-- SHOW NAMESPACES
SHOW NAMESPACES;
SELECT * FROM command('SHOW NAMESPACES');
SELECT * FROM command('SHOW NAMESPACES') WHERE namespace = 'table_valued_command';

-- SHOW TABLES
SHOW TABLES;
SELECT * FROM command('SHOW TABLES');
SELECT * FROM command('SHOW TABLES') WHERE tableName = 'test_show_tables';

-- SHOW TBLPROPERTIES
SHOW TBLPROPERTIES test_show_table_properties;
SELECT * FROM command('SHOW TBLPROPERTIES test_show_table_properties');
SELECT * FROM command('SHOW TBLPROPERTIES test_show_table_properties') WHERE key = 'p1';

-- SHOW PARTITIONS
SHOW PARTITIONS test_show_partitions;
SELECT * FROM command('SHOW PARTITIONS test_show_partitions');
SELECT * FROM command('SHOW PARTITIONS test_show_partitions') WHERE partition = 'c=Us/d=1';

-- SHOW COLUMNS
SHOW COLUMNS in test_show_tables;
SELECT * FROM command('SHOW COLUMNS in test_show_tables');
SELECT * FROM command('SHOW COLUMNS in test_show_tables') WHERE col_name = 'a';

-- Unsupported DDL
SHOW CREATE TABLE test_show_tables;
SELECT * FROM command('SHOW CREATE TABLE test_show_tables');

-- Illegal command content
SELECT * FROM command('SHOW CREATE TABLE');

-- Clean Up
DROP TABLE test_show_partitions;
DROP TABLE test_show_table_properties;
DROP TABLE test_show_tables;
USE default;
DROP NAMESPACE table_valued_command;
