-- try the query without indexing, with manual indexing, and with automatic indexing

-- without indexing
SELECT key, value FROM src WHERE key > 80 AND key < 100 ORDER BY key;

set hive.stats.dbclass=fs;
CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- manual indexing
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_where" SELECT `_bucketname` ,  `_offsets` FROM default__src_src_index__ WHERE key > 80 AND key < 100;
SET hive.index.compact.file=${system:test.tmp.dir}/index_where;
SET hive.optimize.index.filter=false;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;

EXPLAIN SELECT key, value FROM src WHERE key > 80 AND key < 100 ORDER BY key;
SELECT key, value FROM src WHERE key > 80 AND key < 100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- automatic indexing
EXPLAIN SELECT key, value FROM src WHERE key > 80 AND key < 100 ORDER BY key;
SELECT key, value FROM src WHERE key > 80 AND key < 100 ORDER BY key;

DROP INDEX src_index on src;
