-- test that stale indexes are not used

CREATE TABLE temp(key STRING, val STRING) STORED AS TEXTFILE;
INSERT OVERWRITE TABLE temp SELECT * FROM src WHERE key < 50;

-- Build an index on temp.
CREATE INDEX temp_index ON TABLE temp(key) as 'COMPACT' WITH DEFERRED REBUILD;
ALTER INDEX temp_index ON temp REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.index.filter=true;
SET hive.optimize.index.filter.compact.minsize=0;

-- overwrite temp table so index is out of date
INSERT OVERWRITE TABLE temp SELECT * FROM src;

-- should return correct results bypassing index
EXPLAIN SELECT * FROM temp WHERE key  = 86;
SELECT * FROM temp WHERE key  = 86;
DROP table temp;
