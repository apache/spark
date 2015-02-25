-- try the query without indexing, with manual indexing, and with automatic indexing

EXPLAIN SELECT a.key, b.key FROM src a JOIN src b ON (a.value = b.value) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;
SELECT a.key, b.key FROM src a JOIN src b ON (a.value = b.value) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;

set hive.stats.dbclass=fs;
CREATE INDEX src_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

EXPLAIN SELECT a.key, b.key FROM src a JOIN src b ON (a.value = b.value) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;
SELECT a.key, b.key FROM src a JOIN src b ON (a.value = b.value) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;

DROP INDEX src_index on src;
