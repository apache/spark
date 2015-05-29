set hive.stats.dbclass=fs;
drop index src_index on src;

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_src_index__ WHERE key<1000;
SET hive.index.compact.file=${system:test.tmp.dir}/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SET hive.index.compact.query.max.entries=5;
SELECT key, value FROM src WHERE key=100 ORDER BY key;
