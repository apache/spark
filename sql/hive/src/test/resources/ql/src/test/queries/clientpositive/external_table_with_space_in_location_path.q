dfs ${system:test.dfs.mkdir} hdfs:///tmp/test/;

dfs -copyFromLocal ../../data/files/ext_test_space hdfs:///tmp/test/ext_test_space;

CREATE EXTERNAL TABLE spacetest (id int, message string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION 'hdfs:///tmp/test/ext_test_space/folder+with space';

SELECT * FROM spacetest;

SELECT count(*) FROM spacetest;

DROP TABLE spacetest;

CREATE EXTERNAL TABLE spacetestpartition (id int, message string) PARTITIONED BY (day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

ALTER TABLE spacetestpartition ADD PARTITION (day=10) LOCATION 'hdfs:///tmp/test/ext_test_space/folder+with space';

SELECT * FROM spacetestpartition;

SELECT count(*) FROM spacetestpartition;

DROP TABLE spacetestpartition;

dfs -rmr hdfs:///tmp/test;
