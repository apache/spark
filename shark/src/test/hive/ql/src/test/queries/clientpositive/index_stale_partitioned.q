-- Test if index is actually being used.

-- Create temp, and populate it with some values in src.
CREATE TABLE temp(key STRING, val STRING) PARTITIONED BY (foo string) STORED AS TEXTFILE;
ALTER TABLE temp ADD PARTITION (foo = 'bar');
INSERT OVERWRITE TABLE temp PARTITION (foo = 'bar') SELECT * FROM src WHERE key < 50;

-- Build an index on temp.
CREATE INDEX temp_index ON TABLE temp(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX temp_index ON temp PARTITION (foo = 'bar') REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- overwrite temp table so index is out of date
INSERT OVERWRITE TABLE temp PARTITION (foo = 'bar') SELECT * FROM src;

-- query should not return any values
SELECT * FROM default__temp_temp_index__ WHERE key = 86 AND foo='bar';
EXPLAIN SELECT * FROM temp WHERE key  = 86 AND foo = 'bar';
SELECT * FROM temp WHERE key  = 86 AND foo = 'bar';

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=false;
DROP table temp;
