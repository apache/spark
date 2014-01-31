-- This test is used for testing EXPLAIN LOGICAL command

-- Create some views
CREATE VIEW V1 AS SELECT key, value from src;
CREATE VIEW V2 AS SELECT ds, key, value FROM srcpart WHERE ds IS NOT NULL;
CREATE VIEW V3 AS 
  SELECT src1.key, src2.value FROM V2 src1 
  JOIN src src2 ON src1.key = src2.key WHERE src1.ds IS NOT NULL;
CREATE VIEW V4 AS 
  SELECT src1.key, src2.value as value1, src3.value as value2 
  FROM V1 src1 JOIN V2 src2 on src1.key = src2.key JOIN src src3 ON src2.key = src3.key;

-- Simple select queries, union queries and join queries
EXPLAIN LOGICAL 
  SELECT key, count(1) FROM srcpart WHERE ds IS NOT NULL GROUP BY key;
EXPLAIN LOGICAL 
  SELECT key, count(1) FROM (SELECT key, value FROM src) subq1 GROUP BY key;
EXPLAIN LOGICAL 
  SELECT * FROM (
    SELECT key, value FROM src UNION ALL SELECT key, value FROM srcpart WHERE ds IS NOT NULL
  ) S1;
EXPLAIN LOGICAL 
  SELECT S1.key, S2.value FROM src S1 JOIN srcpart S2 ON S1.key = S2.key WHERE ds IS NOT NULL;

-- With views
EXPLAIN LOGICAL SELECT * FROM V1;
EXPLAIN LOGICAL SELECT * FROM V2;
EXPLAIN LOGICAL SELECT * FROM V3;
EXPLAIN LOGICAL SELECT * FROM V4;

-- The table should show up in the explain logical even if none
-- of the partitions are selected.
CREATE VIEW V5 as SELECT * FROM srcpart where ds = '10';
EXPLAIN LOGICAL SELECT * FROM V5;

EXPLAIN LOGICAL SELECT s1.key, s1.cnt, s2.value FROM (SELECT key, count(value) as cnt FROM src GROUP BY key) s1 JOIN src s2 ON (s1.key = s2.key) ORDER BY s1.key;
