-- Test temp table
CREATE TEMPORARY VIEW desc_col_temp_table (key int COMMENT 'column_comment') USING PARQUET;

DESC desc_col_temp_table key;

DESC EXTENDED desc_col_temp_table key;

DESC FORMATTED desc_col_temp_table key;

-- Describe a column with qualified name
DESC FORMATTED desc_col_temp_table desc_col_temp_table.key;

-- Describe a non-existent column
DESC desc_col_temp_table key1;

-- Test persistent table
CREATE TABLE desc_col_table (key int COMMENT 'column_comment') USING PARQUET;

ANALYZE TABLE desc_col_table COMPUTE STATISTICS FOR COLUMNS key;

DESC desc_col_table key;

DESC EXTENDED desc_col_table key;

DESC FORMATTED desc_col_table key;

-- Test complex columns
CREATE TABLE desc_col_complex_table (`a.b` int, col struct<x:int, y:string>) USING PARQUET;

DESC FORMATTED desc_col_complex_table `a.b`;

DESC FORMATTED desc_col_complex_table col;

-- Describe a nested column
DESC FORMATTED desc_col_complex_table col.x;
