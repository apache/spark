dfs ${system:test.dfs.mkdir} hdfs:///tmp/test_load_hdfs_file_with_space_in_the_name/;

dfs -copyFromLocal ../../data/files hdfs:///tmp/test_load_hdfs_file_with_space_in_the_name/.;

CREATE TABLE load_file_with_space_in_the_name(name STRING, age INT);
LOAD DATA INPATH 'hdfs:///tmp/test_load_hdfs_file_with_space_in_the_name/files/person age.txt' INTO TABLE load_file_with_space_in_the_name;
LOAD DATA INPATH 'hdfs:///tmp/test_load_hdfs_file_with_space_in_the_name/files/person+age.txt' INTO TABLE load_file_with_space_in_the_name;

dfs -rmr hdfs:///tmp/test_load_hdfs_file_with_space_in_the_name;

