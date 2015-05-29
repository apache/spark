set hive.stats.dbclass=fs;
CREATE TABLE src_index_test_rc (key int, value string) STORED AS RCFILE;

INSERT OVERWRITE TABLE src_index_test_rc SELECT * FROM src;

CREATE INDEX src_index ON TABLE src_index_test_rc(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src_index_test_rc REBUILD;
SELECT x.* FROM default__src_index_test_rc_src_index__ x ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_result" SELECT `_bucketname` ,  `_offsets` FROM default__src_index_test_rc_src_index__ WHERE key=100;
SET hive.index.compact.file=${system:test.tmp.dir}/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.compact.HiveCompactIndexInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100 ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src_index_test_rc WHERE key=100 ORDER BY key;

DROP INDEX src_index on src_index_test_rc;
DROP TABLE src_index_test_rc;
