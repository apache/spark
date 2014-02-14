set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.enforce.bucketing=true;
set hive.enforce.sorting=true;
set hive.exec.reducers.max = 1;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false; 

-- Create bucketed and sorted tables
CREATE TABLE test_table1 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table2 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table3 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table4 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table5 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table6 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table7 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE test_table8 (key INT, value STRING) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

INSERT OVERWRITE TABLE test_table1
SELECT * FROM src WHERE key < 10;

INSERT OVERWRITE TABLE test_table2
SELECT * FROM src  WHERE key < 10;

INSERT OVERWRITE TABLE test_table3
SELECT * FROM src WHERE key < 10;

INSERT OVERWRITE TABLE test_table4
SELECT * FROM src  WHERE key < 10;

INSERT OVERWRITE TABLE test_table5
SELECT * FROM src WHERE key < 10;

INSERT OVERWRITE TABLE test_table6
SELECT * FROM src  WHERE key < 10;

INSERT OVERWRITE TABLE test_table7
SELECT * FROM src WHERE key < 10;

INSERT OVERWRITE TABLE test_table8
SELECT * FROM src  WHERE key < 10;

-- Mapjoin followed by a aggregation should be performed in a single MR job upto 7 tables
EXPLAIN
SELECT /*+ mapjoin(b, c, d, e, f, g) */ count(*)
FROM test_table1 a JOIN test_table2 b ON a.key = b.key
JOIN test_table3 c ON a.key = c.key
JOIN test_table4 d ON a.key = d.key
JOIN test_table5 e ON a.key = e.key
JOIN test_table6 f ON a.key = f.key
JOIN test_table7 g ON a.key = g.key;

SELECT /*+ mapjoin(b, c, d, e, f, g) */ count(*)
FROM test_table1 a JOIN test_table2 b ON a.key = b.key
JOIN test_table3 c ON a.key = c.key
JOIN test_table4 d ON a.key = d.key
JOIN test_table5 e ON a.key = e.key
JOIN test_table6 f ON a.key = f.key
JOIN test_table7 g ON a.key = g.key;

set hive.auto.convert.join=true;
set hive.auto.convert.sortmerge.join=true;

-- It should be automatically converted to a sort-merge join followed by a groupby in
-- a single MR job
EXPLAIN
SELECT count(*)
FROM test_table1 a LEFT OUTER JOIN test_table2 b ON a.key = b.key
LEFT OUTER JOIN test_table3 c ON a.key = c.key
LEFT OUTER JOIN test_table4 d ON a.key = d.key
LEFT OUTER JOIN test_table5 e ON a.key = e.key
LEFT OUTER JOIN test_table6 f ON a.key = f.key
LEFT OUTER JOIN test_table7 g ON a.key = g.key;

SELECT count(*)
FROM test_table1 a LEFT OUTER JOIN test_table2 b ON a.key = b.key
LEFT OUTER JOIN test_table3 c ON a.key = c.key
LEFT OUTER JOIN test_table4 d ON a.key = d.key
LEFT OUTER JOIN test_table5 e ON a.key = e.key
LEFT OUTER JOIN test_table6 f ON a.key = f.key
LEFT OUTER JOIN test_table7 g ON a.key = g.key;

EXPLAIN
SELECT count(*)
FROM test_table1 a LEFT OUTER JOIN test_table2 b ON a.key = b.key
LEFT OUTER JOIN test_table3 c ON a.key = c.key
LEFT OUTER JOIN test_table4 d ON a.key = d.key
LEFT OUTER JOIN test_table5 e ON a.key = e.key
LEFT OUTER JOIN test_table6 f ON a.key = f.key
LEFT OUTER JOIN test_table7 g ON a.key = g.key
LEFT OUTER JOIN test_table8 h ON a.key = h.key;

SELECT count(*)
FROM test_table1 a LEFT OUTER JOIN test_table2 b ON a.key = b.key
LEFT OUTER JOIN test_table3 c ON a.key = c.key
LEFT OUTER JOIN test_table4 d ON a.key = d.key
LEFT OUTER JOIN test_table5 e ON a.key = e.key
LEFT OUTER JOIN test_table6 f ON a.key = f.key
LEFT OUTER JOIN test_table7 g ON a.key = g.key
LEFT OUTER JOIN test_table8 h ON a.key = h.key;

-- outer join with max 16 aliases
EXPLAIN
SELECT a.*
FROM test_table1 a
LEFT OUTER JOIN test_table2 b ON a.key = b.key
LEFT OUTER JOIN test_table3 c ON a.key = c.key
LEFT OUTER JOIN test_table4 d ON a.key = d.key
LEFT OUTER JOIN test_table5 e ON a.key = e.key
LEFT OUTER JOIN test_table6 f ON a.key = f.key
LEFT OUTER JOIN test_table7 g ON a.key = g.key
LEFT OUTER JOIN test_table8 h ON a.key = h.key
LEFT OUTER JOIN test_table4 i ON a.key = i.key
LEFT OUTER JOIN test_table5 j ON a.key = j.key
LEFT OUTER JOIN test_table6 k ON a.key = k.key
LEFT OUTER JOIN test_table7 l ON a.key = l.key
LEFT OUTER JOIN test_table8 m ON a.key = m.key
LEFT OUTER JOIN test_table7 n ON a.key = n.key
LEFT OUTER JOIN test_table8 o ON a.key = o.key
LEFT OUTER JOIN test_table4 p ON a.key = p.key
LEFT OUTER JOIN test_table5 q ON a.key = q.key
LEFT OUTER JOIN test_table6 r ON a.key = r.key
LEFT OUTER JOIN test_table7 s ON a.key = s.key
LEFT OUTER JOIN test_table8 t ON a.key = t.key;
