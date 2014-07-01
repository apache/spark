-- try the query without indexing, with manual indexing, and with automatic indexing
-- without indexing
SELECT key, value FROM src WHERE key=0 AND value = "val_0" ORDER BY key;

-- create indices
EXPLAIN
CREATE INDEX src1_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
EXPLAIN
CREATE INDEX src2_index ON TABLE src(value) as 'BITMAP' WITH DEFERRED REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
CREATE INDEX src1_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
CREATE INDEX src2_index ON TABLE src(value) as 'BITMAP' WITH DEFERRED REBUILD;
ALTER INDEX src1_index ON src REBUILD;
ALTER INDEX src2_index ON src REBUILD;
SELECT * FROM default__src_src1_index__ ORDER BY key;
SELECT * FROM default__src_src2_index__ ORDER BY value;


-- manual indexing
EXPLAIN
SELECT a.bucketname AS `_bucketname`, COLLECT_SET(a.offset) as `_offsets`
FROM (SELECT `_bucketname` AS bucketname, `_offset` AS offset, `_bitmaps` AS bitmaps FROM default__src_src1_index__
        WHERE key = 0) a
  JOIN 
     (SELECT `_bucketname` AS bucketname, `_offset` AS offset, `_bitmaps` AS bitmaps FROM default__src_src2_index__
        WHERE value = "val_0") b
  ON
    a.bucketname = b.bucketname AND a.offset = b.offset WHERE NOT
EWAH_BITMAP_EMPTY(EWAH_BITMAP_AND(a.bitmaps, b.bitmaps)) GROUP BY a.bucketname;

INSERT OVERWRITE DIRECTORY "${system:test.tmp.dir}/index_result" 
SELECT a.bucketname AS `_bucketname`, COLLECT_SET(a.offset) as `_offsets`
FROM (SELECT `_bucketname` AS bucketname, `_offset` AS offset, `_bitmaps` AS bitmaps FROM default__src_src1_index__
        WHERE key = 0) a
  JOIN 
     (SELECT `_bucketname` AS bucketname, `_offset` AS offset, `_bitmaps` AS bitmaps FROM default__src_src2_index__
        WHERE value = "val_0") b
  ON
    a.bucketname = b.bucketname AND a.offset = b.offset WHERE NOT
EWAH_BITMAP_EMPTY(EWAH_BITMAP_AND(a.bitmaps, b.bitmaps)) GROUP BY a.bucketname;

SELECT key, value FROM src WHERE key=0 AND value = "val_0" ORDER BY key;


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SELECT key, value FROM src WHERE key=0 AND value = "val_0" ORDER BY key;

DROP INDEX src1_index ON src;
DROP INDEX src2_index ON src;

