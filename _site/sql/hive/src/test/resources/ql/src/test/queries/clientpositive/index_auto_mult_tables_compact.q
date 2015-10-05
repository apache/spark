-- try the query without indexing, with manual indexing, and with automatic indexing

-- without indexing
EXPLAIN SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;
SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;

set hive.stats.dbclass=fs;

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

CREATE INDEX srcpart_index ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX srcpart_index ON srcpart REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- automatic indexing
EXPLAIN SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;
SELECT a.key, a.value FROM src a JOIN srcpart b ON (a.key = b.key) WHERE a.key > 80 AND a.key < 100 AND b.key > 70 AND b.key < 90 ORDER BY a.key;

DROP INDEX src_index on src;
DROP INDEX srcpart_index on src;
