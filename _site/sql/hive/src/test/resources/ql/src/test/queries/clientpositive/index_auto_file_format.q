set hive.stats.dbclass=fs;
-- test automatic use of index on different file formats
CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

EXPLAIN SELECT key, value FROM src WHERE key=86 ORDER BY key;
SELECT key, value FROM src WHERE key=86 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

EXPLAIN SELECT key, value FROM src WHERE key=86 ORDER BY key;
SELECT key, value FROM src WHERE key=86 ORDER BY key;

DROP INDEX src_index on src;
