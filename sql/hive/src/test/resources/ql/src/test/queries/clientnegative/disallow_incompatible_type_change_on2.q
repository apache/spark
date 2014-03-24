SET hive.metastore.disallow.incompatible.col.type.changes=true;
SELECT * FROM src LIMIT 1;
CREATE TABLE test_table123 (a INT, b STRING) PARTITIONED BY (ds STRING) STORED AS SEQUENCEFILE;
INSERT OVERWRITE TABLE test_table123 PARTITION(ds="foo1") SELECT 1, "one" FROM src LIMIT 1;
SELECT * from test_table123 WHERE ds="foo1";
ALTER TABLE test_table123 CHANGE COLUMN b b MAP<STRING, STRING>;
