-- Tests for _metadata virtual column resolution across different file formats,
-- qualifications, clauses, and join types.
-- t1 uses PARQUET, t2 uses CSV, t3 uses ORC.

CREATE DATABASE IF NOT EXISTS testdb;
USE testdb;

CREATE TABLE t1(id INT, name STRING, value DOUBLE) USING PARQUET;
CREATE TABLE t2(id INT, category STRING, amount BIGINT) USING CSV;
CREATE TABLE t3(a INT, b STRING, c DOUBLE) USING ORC;

-- Basic metadata column selection
SELECT _metadata.file_name FROM t1;
SELECT _metadata.file_path FROM t1;
SELECT _metadata.file_size FROM t1;
SELECT _metadata.file_modification_time FROM t1;
SELECT _metadata.row_index FROM t1;
SELECT _metadata.file_block_start FROM t1;
SELECT _metadata.file_block_length FROM t1;

-- Qualified table references with metadata
SELECT t1._metadata.file_name FROM t1;
SELECT testdb.t1._metadata.file_name FROM testdb.t1;

-- Mixed qualified and unqualified metadata references
SELECT _metadata.file_name, t1._metadata.file_path FROM t1;
SELECT t1._metadata.file_name, testdb.t1._metadata.file_path FROM testdb.t1;

-- Metadata in WHERE clause
SELECT * FROM t1 WHERE _metadata.file_size > 1000;
SELECT id, name FROM t1 WHERE _metadata.row_index < 100;
SELECT * FROM t1 WHERE _metadata.file_name = 'data.parquet';
SELECT * FROM t1 WHERE _metadata.file_modification_time > timestamp'2024-01-01 00:00:00';

-- Qualified metadata in WHERE clause
SELECT * FROM t1 WHERE t1._metadata.file_size > 1000;
SELECT * FROM testdb.t1 WHERE testdb.t1._metadata.row_index < 100;

-- Metadata in ORDER BY clause
SELECT id, name, _metadata.file_name FROM t1 ORDER BY _metadata.file_size;
SELECT * FROM t1 ORDER BY _metadata.row_index DESC;
SELECT id, _metadata.file_name FROM t1 ORDER BY _metadata.file_modification_time, _metadata.row_index;

-- Qualified metadata in GROUP BY
SELECT t1._metadata.file_name, COUNT(*) FROM t1 GROUP BY t1._metadata.file_name;
SELECT testdb.t1._metadata.file_path, SUM(value) FROM testdb.t1 GROUP BY testdb.t1._metadata.file_path;

-- Metadata with aggregations
SELECT _metadata.file_name, MIN(_metadata.row_index), MAX(_metadata.row_index) FROM t1
GROUP BY _metadata.file_name;
SELECT _metadata.file_path, COUNT(DISTINCT _metadata.file_name) FROM t1
GROUP BY _metadata.file_path;

-- Metadata in JOIN conditions
SELECT t1.id, t2.category, t1._metadata.file_name
FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, t1._metadata.file_name, t2._metadata.file_name
FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, t1._metadata.row_index, t2._metadata.row_index
FROM t1 JOIN t2 ON t1.id = t2.id
WHERE t1._metadata.file_size > 1000;

-- Metadata in LEFT/RIGHT JOIN
SELECT t1.name, t1._metadata.file_name, t2._metadata.file_name
FROM t1 LEFT JOIN t2 ON t1.id = t2.id;

SELECT t1._metadata.file_path, t2._metadata.file_path, t1.id
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id;

-- Metadata in FULL OUTER JOIN
SELECT t1._metadata.file_name, t2._metadata.file_name, t1.id, t2.id
FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;

-- Metadata in self-join
SELECT a.id, a._metadata.file_name, b._metadata.file_name
FROM t1 a JOIN t1 b ON a.id = b.id
WHERE a._metadata.row_index < b._metadata.row_index;

-- Metadata with table aliases
SELECT t._metadata.file_name, t.id FROM t1 t;
SELECT x._metadata.file_name, x._metadata.row_index FROM t1 x
WHERE x._metadata.file_size > 500;

-- Metadata in subqueries
SELECT * FROM (SELECT id, _metadata.file_name FROM t1) sub;
SELECT id FROM t1 WHERE _metadata.file_name IN
                        (SELECT _metadata.file_name FROM t2 WHERE amount > 100);

-- Metadata in UNION
SELECT _metadata.file_name, id FROM t1
UNION
SELECT _metadata.file_name, id FROM t2;

-- === _metadata.* struct expansion ===

-- expand all metadata fields for each format
SELECT _metadata.* FROM t1;
SELECT _metadata.* FROM t2;
SELECT _metadata.* FROM t3;

-- qualified _metadata.* expansion
SELECT t1._metadata.* FROM t1;
SELECT t2._metadata.* FROM t2;
SELECT t3._metadata.* FROM t3;

-- _metadata.* alongside regular columns (parquet)
SELECT id, name, _metadata.* FROM t1 ORDER BY id;

-- _metadata.* alongside regular columns (csv)
SELECT id, category, _metadata.* FROM t2 ORDER BY id;

-- _metadata.* in subquery
SELECT sub.file_name, sub.file_size FROM (SELECT _metadata.* FROM t1) sub ORDER BY sub.file_name;

-- _metadata.* with WHERE on expanded fields
SELECT id, _metadata.* FROM t1 WHERE _metadata.file_size >= 0 ORDER BY id;

-- _metadata.* in Aggregate
SELECT _metadata.*, COUNT(*) AS cnt FROM t1 GROUP BY ALL ORDER BY _metadata.file_name;
SELECT _metadata.*, COUNT(*) AS cnt FROM t2 GROUP BY ALL ORDER BY _metadata.file_name;
SELECT _metadata.*, COUNT(*) AS cnt FROM t3 GROUP BY ALL ORDER BY _metadata.file_name;

-- Cleanup
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP DATABASE testdb;
