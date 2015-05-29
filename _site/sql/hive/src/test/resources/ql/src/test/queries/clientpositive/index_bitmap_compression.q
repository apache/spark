set hive.stats.dbclass=fs;
SET hive.exec.compress.result=true;
CREATE INDEX src_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- automatic indexing
EXPLAIN SELECT key, value FROM src WHERE key > 80 AND key < 100 ORDER BY key;
SELECT key, value FROM src WHERE key > 80 AND key < 100 ORDER BY key;

DROP INDEX src_index on src;
