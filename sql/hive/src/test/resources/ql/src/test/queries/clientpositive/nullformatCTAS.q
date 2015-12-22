-- base table with null data
DROP TABLE IF EXISTS base_tab;
CREATE TABLE base_tab(a STRING, b STRING, c STRING, d STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/null.txt' INTO TABLE base_tab;
DESCRIBE EXTENDED base_tab;

-- table with non-default null format
DROP TABLE IF EXISTS null_tab3;
EXPLAIN CREATE TABLE null_tab3 ROW FORMAT DELIMITED NULL DEFINED AS 'fooNull'
   AS SELECT a, b FROM base_tab;
CREATE TABLE null_tab3 ROW FORMAT DELIMITED NULL DEFINED AS 'fooNull'
   AS SELECT a, b FROM base_tab;
DESCRIBE EXTENDED null_tab3;
SHOW CREATE TABLE null_tab3;

dfs -cat ${system:test.warehouse.dir}/null_tab3/*;
SELECT * FROM null_tab3;
-- alter the null format and verify that the old null format is no longer in effect
ALTER TABLE null_tab3 SET SERDEPROPERTIES ( 'serialization.null.format'='foo');
SELECT * FROM null_tab3;


DROP TABLE null_tab3;
DROP TABLE base_tab;
