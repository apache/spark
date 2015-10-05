-- Test if index is actually being used.

-- Create temp, and populate it with some values in src.
CREATE TABLE temp(key STRING, val STRING) STORED AS TEXTFILE;
INSERT OVERWRITE TABLE temp SELECT * FROM src WHERE key < 50;

-- Build an index on temp.
CREATE INDEX temp_index ON TABLE temp(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX temp_index ON temp REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.autoupdate=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- overwrite temp table so index is out of date
EXPLAIN INSERT OVERWRITE TABLE temp SELECT * FROM src;
INSERT OVERWRITE TABLE temp SELECT * FROM src;

-- query should return indexed values
EXPLAIN SELECT * FROM temp WHERE key  = 86;
SELECT * FROM temp WHERE key  = 86;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=false;
DROP table temp;
