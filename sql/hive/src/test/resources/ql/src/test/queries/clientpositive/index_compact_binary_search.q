SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.default.fileformat=TextFile;
set hive.stats.dbclass=fs;
CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=1;
SET hive.index.compact.binary.search=true;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHiveSortedInputFormatUsedHook;

SELECT * FROM src WHERE key = '0';

SELECT * FROM src WHERE key < '1';

SELECT * FROM src WHERE key <= '0';

SELECT * FROM src WHERE key > '8';

SELECT * FROM src WHERE key >= '9';

SET hive.exec.post.hooks=;

DROP INDEX src_index ON src;

SET hive.default.fileformat=RCFILE;

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHiveSortedInputFormatUsedHook;

SELECT * FROM src WHERE key = '0';

SELECT * FROM src WHERE key < '1';

SELECT * FROM src WHERE key <= '0';

SELECT * FROM src WHERE key > '8';

SELECT * FROM src WHERE key >= '9';

SET hive.exec.post.hooks=;

DROP INDEX src_index ON src;

SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.default.fileformat=TextFile;

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHiveSortedInputFormatUsedHook;

SELECT * FROM src WHERE key = '0';

SELECT * FROM src WHERE key < '1';

SELECT * FROM src WHERE key <= '0';

SELECT * FROM src WHERE key > '8';

SELECT * FROM src WHERE key >= '9';

SET hive.exec.post.hooks=;

DROP INDEX src_index ON src;

SET hive.default.fileformat=RCFILE;

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHiveSortedInputFormatUsedHook;

SELECT * FROM src WHERE key = '0';

SELECT * FROM src WHERE key < '1';

SELECT * FROM src WHERE key <= '0';

SELECT * FROM src WHERE key > '8';

SELECT * FROM src WHERE key >= '9';

SET hive.exec.post.hooks=;

DROP INDEX src_index ON src;

SET hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
SET hive.default.fileformat=TextFile;

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHiveSortedInputFormatUsedHook;

SELECT * FROM src WHERE key = '0';

SELECT * FROM src WHERE key < '1';

SELECT * FROM src WHERE key <= '0';

SELECT * FROM src WHERE key > '8';

SELECT * FROM src WHERE key >= '9';

SET hive.exec.post.hooks=;

DROP INDEX src_index ON src;

SET hive.default.fileformat=RCFILE;

CREATE INDEX src_index ON TABLE src(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX src_index ON src REBUILD;

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyHiveSortedInputFormatUsedHook;

SELECT * FROM src WHERE key = '0';

SELECT * FROM src WHERE key < '1';

SELECT * FROM src WHERE key <= '0';

SELECT * FROM src WHERE key > '8';

SELECT * FROM src WHERE key >= '9';

SET hive.exec.post.hooks=;

DROP INDEX src_index ON src;
