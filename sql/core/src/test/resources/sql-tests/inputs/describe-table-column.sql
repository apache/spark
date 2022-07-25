-- Test temp table
CREATE TEMPORARY VIEW desc_col_temp_view (key int COMMENT 'column_comment', col struct<x:int, y:string>) USING PARQUET;

-- Describe a nested column
DESC desc_col_temp_view col.x;

-- Test complex columns
CREATE TABLE desc_complex_col_table (`a.b` int, col struct<x:int, y:string>) USING PARQUET;

DESC FORMATTED desc_complex_col_table `a.b`;

DESC FORMATTED desc_complex_col_table col;

-- Describe a nested column
DESC FORMATTED desc_complex_col_table col.x;

