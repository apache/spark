-- create database with multiple tables, indexes and views.
-- Use both partitioned and non-partitioned tables, as well as
-- tables and indexes with specific storage locations
-- verify the drop the database with cascade works and that the directories
-- outside the database's default storage are removed as part of the drop

CREATE DATABASE db5;
SHOW DATABASES;
USE db5;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/temp;
dfs -rmr ${system:test.tmp.dir}/dbcascade;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade;

-- add a table, index and view
CREATE TABLE temp_tbl (id INT, name STRING);
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE temp_tbl;
CREATE VIEW temp_tbl_view AS SELECT * FROM temp_tbl;
CREATE INDEX idx1 ON TABLE temp_tbl(id) AS 'COMPACT' with DEFERRED REBUILD;
ALTER INDEX idx1 ON temp_tbl REBUILD;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/temp_tbl2;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/temp_tbl2_idx2;
-- add a table, index and view with a different storage location
CREATE TABLE temp_tbl2 (id INT, name STRING) LOCATION 'file:${system:test.tmp.dir}/dbcascade/temp_tbl2';
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' into table temp_tbl2;
CREATE VIEW temp_tbl2_view AS SELECT * FROM temp_tbl2;
CREATE INDEX idx2 ON TABLE temp_tbl2(id) AS 'COMPACT' with DEFERRED REBUILD LOCATION 'file:${system:test.tmp.dir}/dbcascade/temp_tbl2_idx2';
ALTER INDEX idx2 ON temp_tbl2 REBUILD;

-- add a partitioned table, index and view
CREATE TABLE part_tab (id INT, name STRING)  PARTITIONED BY (ds string);
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE part_tab PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE part_tab PARTITION (ds='2009-04-09');
CREATE INDEX idx3 ON TABLE part_tab(id) AS 'COMPACT' with DEFERRED REBUILD;
ALTER INDEX idx3 ON part_tab PARTITION (ds='2008-04-09') REBUILD;
ALTER INDEX idx3 ON part_tab PARTITION (ds='2009-04-09') REBUILD;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab2;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab2_idx4;
-- add a partitioned table, index and view with a different storage location
CREATE TABLE part_tab2 (id INT, name STRING)  PARTITIONED BY (ds string)
		LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab2';
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE part_tab2 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE part_tab2 PARTITION (ds='2009-04-09');
CREATE INDEX idx4 ON TABLE part_tab2(id) AS 'COMPACT' with DEFERRED REBUILD
		LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab2_idx4';
ALTER INDEX idx4 ON part_tab2 PARTITION (ds='2008-04-09') REBUILD;
ALTER INDEX idx4 ON part_tab2 PARTITION (ds='2009-04-09') REBUILD;


dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab3;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab3_p1;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab3_idx5;
-- add a partitioned table, index and view with a different storage location
CREATE TABLE part_tab3 (id INT, name STRING)  PARTITIONED BY (ds string)
		LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab3';
ALTER TABLE part_tab3 ADD PARTITION  (ds='2007-04-09') LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab3_p1';
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE part_tab3 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE part_tab3 PARTITION (ds='2009-04-09');
CREATE INDEX idx5 ON TABLE part_tab3(id) AS 'COMPACT' with DEFERRED REBUILD
		LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab3_idx5';
ALTER INDEX idx5 ON part_tab3 PARTITION (ds='2008-04-09') REBUILD;
ALTER INDEX idx5 ON part_tab3 PARTITION (ds='2009-04-09') REBUILD;



dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/extab1;
dfs -touchz ${system:test.tmp.dir}/dbcascade/extab1/file1.txt;
-- add an external table
CREATE EXTERNAL TABLE extab1(id INT, name STRING) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY ''
              LINES TERMINATED BY '\n'
              STORED AS TEXTFILE
              LOCATION 'file:${system:test.tmp.dir}/dbcascade/extab1';

-- drop the database with cascade
DROP DATABASE db5 CASCADE;

dfs -test -d ${system:test.tmp.dir}/dbcascade/extab1;
dfs -rmr ${system:test.tmp.dir}/dbcascade;
