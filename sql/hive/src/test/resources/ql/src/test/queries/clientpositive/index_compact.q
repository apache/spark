set hive.stats.dbclass=fs;
DROP INDEX srcpart_index_proj on srcpart;

EXPLAIN
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX srcpart_index_proj ON srcpart REBUILD;
SELECT x.* FROM default__srcpart_srcpart_index_proj__ x WHERE x.ds = '2008-04-08' and x.hr = 11 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_test_index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ x WHERE x.key=100 AND x.ds = '2008-04-08';
SET hive.index.compact.file=${system:test.tmp.dir}/index_test_index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_test_index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ x WHERE x.key=100 AND x.ds = '2008-04-08' and x.hr = 11;
SET hive.index.compact.file=${system:test.tmp.dir}/index_test_index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' and hr = 11 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM srcpart WHERE key=100 AND ds = '2008-04-08' and hr = 11 ORDER BY key;

DROP INDEX srcpart_index_proj on srcpart;

EXPLAIN
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
CREATE INDEX srcpart_index_proj ON TABLE srcpart(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER  INDEX srcpart_index_proj ON srcpart REBUILD;
SELECT x.* FROM default__srcpart_srcpart_index_proj__ x;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__srcpart_srcpart_index_proj__ WHERE key=100;
SET hive.index.compact.file=${system:test.tmp.dir}/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM srcpart WHERE key=100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM srcpart WHERE key=100 ORDER BY key;

DROP INDEX srcpart_index_proj on srcpart;
