-- base table with null data
DROP TABLE IF EXISTS base_tab;
CREATE TABLE base_tab(a STRING, b STRING, c STRING, d STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/null.txt' INTO TABLE base_tab;
DESCRIBE EXTENDED base_tab;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hive_test/nullformat/tmp;
dfs -rmr ${system:test.tmp.dir}/hive_test/nullformat/*;
INSERT OVERWRITE LOCAL DIRECTORY '${system:test.tmp.dir}/hive_test/nullformat'
   ROW FORMAT DELIMITED NULL DEFINED AS 'fooNull' SELECT a,b FROM base_tab;
dfs -cat ${system:test.tmp.dir}/hive_test/nullformat/000000_0;

-- load the exported data back into a table with same null format and verify null values
DROP TABLE IF EXISTS null_tab2;
CREATE TABLE null_tab2(a STRING, b STRING) ROW FORMAT DELIMITED NULL DEFINED AS 'fooNull';
LOAD DATA LOCAL INPATH '${system:test.tmp.dir}/hive_test/nullformat/000000_0' INTO TABLE null_tab2;
SELECT * FROM null_tab2;


dfs -rmr ${system:test.tmp.dir}/hive_test/nullformat;
DROP TABLE base_tab;
