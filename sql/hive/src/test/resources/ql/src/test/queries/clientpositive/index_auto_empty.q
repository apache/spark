-- Test to ensure that an empty index result is propagated correctly

-- Create temp, and populate it with some values in src.
CREATE TABLE temp(key STRING, val STRING) STORED AS TEXTFILE;

-- Build an index on temp.
CREATE INDEX temp_index ON TABLE temp(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX temp_index ON temp REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- query should not return any values
SELECT * FROM default__temp_temp_index__ WHERE key = 86;
EXPLAIN SELECT * FROM temp WHERE key  = 86;
SELECT * FROM temp WHERE key  = 86;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=false;
DROP table temp;
