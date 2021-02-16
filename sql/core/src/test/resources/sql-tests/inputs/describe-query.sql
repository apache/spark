-- Test tables
CREATE table  desc_temp1 (key int COMMENT 'column_comment', val string) USING PARQUET;
CREATE table  desc_temp2 (key int, val string) USING PARQUET;

-- Simple Describe query
DESC SELECT key, key + 1 as plusone FROM desc_temp1;
DESC QUERY SELECT * FROM desc_temp2;
DESC SELECT key, COUNT(*) as count FROM desc_temp1 group by key;
DESC SELECT 10.00D as col1;
DESC QUERY SELECT key FROM desc_temp1 UNION ALL select CAST(1 AS DOUBLE);
DESC QUERY VALUES(1.00D, 'hello') as tab1(col1, col2);
DESC QUERY FROM desc_temp1 a SELECT *;
DESC WITH s AS (SELECT 'hello' as col1) SELECT * FROM s;
DESCRIBE QUERY WITH s AS (SELECT * from desc_temp1) SELECT * FROM s;
DESCRIBE SELECT * FROM (FROM desc_temp2 select * select *);

-- Error cases.
DESCRIBE INSERT INTO desc_temp1 values (1, 'val1');
DESCRIBE INSERT INTO desc_temp1 SELECT * FROM desc_temp2;
DESCRIBE
   FROM desc_temp1 a
     insert into desc_temp1 select *
     insert into desc_temp2 select *;

-- Explain
EXPLAIN DESC QUERY SELECT * FROM desc_temp2 WHERE key > 0;
EXPLAIN EXTENDED DESC WITH s AS (SELECT 'hello' as col1) SELECT * FROM s;

-- cleanup
DROP TABLE desc_temp1;
DROP TABLE desc_temp2;
