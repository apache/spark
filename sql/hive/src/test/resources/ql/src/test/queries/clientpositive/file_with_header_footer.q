dfs ${system:test.dfs.mkdir} hdfs:///tmp/test/;

dfs -copyFromLocal ../../data/files/header_footer_table_1 hdfs:///tmp/test/header_footer_table_1;

dfs -copyFromLocal ../../data/files/header_footer_table_2 hdfs:///tmp/test/header_footer_table_2;

dfs -copyFromLocal ../../data/files/header_footer_table_3 hdfs:///tmp/test/header_footer_table_3;

CREATE EXTERNAL TABLE header_footer_table_1 (name string, message string, id int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 'hdfs:///tmp/test/header_footer_table_1' tblproperties ("skip.header.line.count"="1", "skip.footer.line.count"="2");

SELECT * FROM header_footer_table_1;

SELECT * FROM header_footer_table_1 WHERE id < 50;

CREATE EXTERNAL TABLE header_footer_table_2 (name string, message string, id int) PARTITIONED BY (year int, month int, day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' tblproperties ("skip.header.line.count"="1", "skip.footer.line.count"="2");

ALTER TABLE header_footer_table_2 ADD PARTITION (year=2012, month=1, day=1) location 'hdfs:///tmp/test/header_footer_table_2/2012/01/01';

ALTER TABLE header_footer_table_2 ADD PARTITION (year=2012, month=1, day=2) location 'hdfs:///tmp/test/header_footer_table_2/2012/01/02';

ALTER TABLE header_footer_table_2 ADD PARTITION (year=2012, month=1, day=3) location 'hdfs:///tmp/test/header_footer_table_2/2012/01/03';

SELECT * FROM header_footer_table_2;

SELECT * FROM header_footer_table_2 WHERE id < 50;

CREATE EXTERNAL TABLE emptytable (name string, message string, id int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 'hdfs:///tmp/test/header_footer_table_3' tblproperties ("skip.header.line.count"="1", "skip.footer.line.count"="2");

SELECT * FROM emptytable;

SELECT * FROM emptytable WHERE id < 50;

DROP TABLE header_footer_table_1;

DROP TABLE header_footer_table_2;

DROP TABLE emptytable;

dfs -rmr hdfs:///tmp/test;