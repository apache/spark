SET hive.metastore.disallow.incompatible.col.type.changes=false;
SELECT * FROM src LIMIT 1;
CREATE TABLE test_table123 (a INT, b MAP<STRING, STRING>) PARTITIONED BY (ds STRING) STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE test_table123 PARTITION(ds="foo1") SELECT 1, MAP("a1", "b1") FROM src LIMIT 1;
SELECT * from test_table123 WHERE ds="foo1";
-- This should now work as hive.metastore.disallow.incompatible.col.type.changes is false
ALTER TABLE test_table123 REPLACE COLUMNS (a INT, b STRING);
