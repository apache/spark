set hive.stats.dbclass=fs;
-- test cases where the index should not be used automatically

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=5368709120;
SET hive.optimize.index.filter.compact.maxsize=-1;

-- min size too large (src is less than 5G)
EXPLAIN SELECT * FROM src WHERE key > 80 AND key < 100 ORDER BY key;
SELECT * FROM src WHERE key > 80 AND key < 100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;
SET hive.optimize.index.filter.compact.maxsize=1;

-- max size too small
EXPLAIN SELECT * FROM src WHERE key > 80 AND key < 100 ORDER BY key;
SELECT * FROM src WHERE key > 80 AND key < 100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;
SET hive.optimize.index.filter.compact.maxsize=-1;

-- OR predicate not supported by compact indexes
EXPLAIN SELECT * FROM src WHERE key < 10 OR key > 480 ORDER BY key;
SELECT * FROM src WHERE key < 10 OR key > 480 ORDER BY key;

 SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;
SET hive.optimize.index.filter.compact.maxsize=-1;

-- columns are not covered by indexes
DROP INDEX src_index on src;
CREATE INDEX src_val_index ON TABLE src(value) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_val_index ON src REBUILD;

EXPLAIN SELECT * FROM src WHERE key > 80 AND key < 100 ORDER BY key;
SELECT * FROM src WHERE key > 80 AND key < 100 ORDER BY key;

DROP INDEX src_val_index on src;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;
SET hive.optimize.index.filter.compact.maxsize=-1;

-- required partitions have not been built yet
CREATE INDEX src_part_index ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_part_index ON srcpart PARTITION (ds='2008-04-08', hr=11) REBUILD;

EXPLAIN SELECT * FROM srcpart WHERE ds='2008-04-09' AND hr=12 AND key < 10 ORDER BY key;
SELECT * FROM srcpart WHERE ds='2008-04-09' AND hr=12 AND key < 10 ORDER BY key;

DROP INDEX src_part_index on srcpart;
