set hive.stats.dbclass=fs;
EXPLAIN
CREATE INDEX src1_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
EXPLAIN
CREATE INDEX src2_index ON TABLE src(value) as 'BITMAP' WITH DEFERRED REBUILD;

CREATE INDEX src1_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
CREATE INDEX src2_index ON TABLE src(value) as 'BITMAP' WITH DEFERRED REBUILD;
ALTER INDEX src1_index ON src REBUILD;
ALTER INDEX src2_index ON src REBUILD;
SELECT * FROM default__src_src1_index__ ORDER BY key;
SELECT * FROM default__src_src2_index__ ORDER BY value;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_result" 
SELECT t.bucketname as `_bucketname`, COLLECT_SET(t.offset) AS `_offsets` FROM
  (SELECT `_bucketname` AS bucketname, `_offset` AS offset
      FROM default__src_src1_index__ 
      WHERE key = 0 AND NOT EWAH_BITMAP_EMPTY(`_bitmaps`) UNION ALL
   SELECT `_bucketname` AS bucketname, `_offset` AS offset
      FROM default__src_src2_index__
      WHERE value = "val2" AND NOT EWAH_BITMAP_EMPTY(`_bitmaps`)) t
GROUP BY t.bucketname;

SET hive.index.blockfilter.file=${system:test.tmp.dir}/index_result;
SET hive.input.format=org.apache.hadoop.hive.ql.index.HiveIndexedInputFormat;

SELECT key, value FROM src WHERE key=0 OR value = "val_2" ORDER BY key;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SELECT key, value FROM src WHERE key=0 OR value = "val_2" ORDER BY key;

DROP INDEX src1_index ON src;
DROP INDEX src2_index ON src;

