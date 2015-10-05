EXPLAIN
CREATE INDEX src1_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.map.aggr=false;
CREATE INDEX src1_index ON TABLE src(key) as 'BITMAP' WITH DEFERRED REBUILD;
ALTER INDEX src1_index ON src REBUILD;
