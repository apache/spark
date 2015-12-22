dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_file_with_header_footer_negative/;

dfs -copyFromLocal ../data/files/header_footer_table_1 hdfs:///tmp/test_file_with_header_footer_negative/header_footer_table_1;

dfs -copyFromLocal ../data/files/header_footer_table_2 hdfs:///tmp/test_file_with_header_footer_negative/header_footer_table_2;

CREATE EXTERNAL TABLE header_footer_table_1 (name string, message string, id int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 'hdfs:///tmp/test_file_with_header_footer_negative/header_footer_table_1' tblproperties ("skip.header.line.count"="1", "skip.footer.line.count"="200");

SELECT * FROM header_footer_table_1;

DROP TABLE header_footer_table_1;

dfs -rmr hdfs:///tmp/test_file_with_header_footer_negative;