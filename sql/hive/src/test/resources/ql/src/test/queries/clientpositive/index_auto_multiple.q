set hive.stats.dbclass=fs;
-- With multiple indexes, make sure we choose which to use in a consistent order

CREATE INDEX src_key_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
CREATE INDEX src_val_index ON TABLE src(value) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_key_index ON src REBUILD;
ALTER INDEX src_val_index ON src REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

EXPLAIN SELECT key, value FROM src WHERE key=86 ORDER BY key;
SELECT key, value FROM src WHERE key=86 ORDER BY key;

DROP INDEX src_key_index ON src;
DROP INDEX src_val_index ON src;
