EXPLAIN
CREATE INDEX src_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
CREATE INDEX src_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;
SELECT x.* FROM default__src_src_index__ x ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_result" SELECT `_bucketname`,
COLLECT_SET(`_offset`) as `_offsets` FROM default__src_src_index__ WHERE NOT
EWAH_BITMAP_EMPTY(`_bitmaps`) AND key=100 GROUP BY `_bucketname`;
SET hive.index.blockfilter.file=${system:test.tmp.dir}/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.HiveIndexedInputFormat;
SELECT key, value FROM src WHERE key=100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src WHERE key=100 ORDER BY key;

DROP INDEX src_index ON src;
