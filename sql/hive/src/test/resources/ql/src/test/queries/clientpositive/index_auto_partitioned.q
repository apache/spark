-- test automatic use of index on table with partitions
CREATE INDEX src_part_index ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_part_index ON srcpart REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

EXPLAIN SELECT key, value FROM srcpart WHERE key=86 AND ds='2008-04-09' ORDER BY key;
SELECT key, value FROM srcpart WHERE key=86 AND ds='2008-04-09' ORDER BY key;

DROP INDEX src_part_index ON srcpart;
