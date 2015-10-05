CREATE DATABASE db1;
CREATE TABLE db1.t1(key1 INT, value1 STRING) PARTITIONED BY (ds STRING, part STRING);

-- describe database.table.column
DESCRIBE db1.t1.key1;
