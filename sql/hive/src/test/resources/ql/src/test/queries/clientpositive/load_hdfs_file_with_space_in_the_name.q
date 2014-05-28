dfs -mkdir hdfs:///tmp/test/;

dfs -copyFromLocal ../data/files hdfs:///tmp/test/.;

CREATE TABLE load_file_with_space_in_the_name(name STRING, age INT);
LOAD DATA INPATH 'hdfs:///tmp/test/files/person age.txt' INTO TABLE load_file_with_space_in_the_name;

dfs -rmr hdfs:///tmp/test;

