set hive.mapred.supports.subdirectories=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false; 
set mapred.input.dir.recursive=true;
set hive.stats.dbclass=fs;
-- Tests truncating a column from a list bucketing table

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

CREATE TABLE test_tab (key STRING, value STRING) PARTITIONED BY (part STRING) STORED AS RCFILE;

ALTER TABLE test_tab SKEWED BY (key) ON ("484") STORED AS DIRECTORIES;

INSERT OVERWRITE TABLE test_tab PARTITION (part = '1') SELECT * FROM src;

describe formatted test_tab partition (part='1');

set hive.stats.dbclass=jdbc:derby;
