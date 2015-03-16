-- base table with null data
DROP TABLE IF EXISTS base_tab;
CREATE TABLE base_tab(a STRING, b STRING, c STRING, d STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/null.txt' INTO TABLE base_tab;
DESCRIBE EXTENDED base_tab;

-- table with non-default null format
DROP TABLE IF EXISTS null_tab1;
EXPLAIN CREATE TABLE null_tab1(a STRING, b STRING) ROW FORMAT DELIMITED NULL DEFINED AS 'fooNull';
CREATE TABLE null_tab1(a STRING, b STRING) ROW FORMAT DELIMITED NULL DEFINED AS 'fooNull';
DESCRIBE EXTENDED null_tab1;
SHOW CREATE TABLE null_tab1;

-- load null data from another table and verify that the null is stored in the expected format
INSERT OVERWRITE TABLE null_tab1 SELECT a,b FROM base_tab;
dfs -cat ${system:test.warehouse.dir}/null_tab1/*;
SELECT * FROM null_tab1;
-- alter the null format and verify that the old null format is no longer in effect
ALTER TABLE null_tab1 SET SERDEPROPERTIES ( 'serialization.null.format'='foo');
SELECT * FROM null_tab1;


DROP TABLE null_tab1;
DROP TABLE base_tab;
