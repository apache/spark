set hive.mapred.supports.subdirectories=true;
set hive.merge.mapfiles=false;	
set hive.merge.mapredfiles=false; 
set mapred.input.dir.recursive=true;

-- This tests that bucketing/sorting metadata is not inferred for tables with list bucketing

-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.23)

-- create a skewed table
CREATE TABLE list_bucketing_table (key STRING, value STRING) 
PARTITIONED BY (part STRING) 
SKEWED BY (key) ON ("484")
STORED AS DIRECTORIES;

-- Tests group by, the output should neither be bucketed nor sorted

INSERT OVERWRITE TABLE list_bucketing_table PARTITION (part = '1')
SELECT key, count(*) FROM src GROUP BY key;

DESC FORMATTED list_bucketing_table PARTITION (part = '1');

-- create a table skewed on a key which doesnt exist in the data
CREATE TABLE list_bucketing_table2 (key STRING, value STRING) 
PARTITIONED BY (part STRING) 
SKEWED BY (key) ON ("abc")
STORED AS DIRECTORIES;

-- should not be bucketed or sorted
INSERT OVERWRITE TABLE list_bucketing_table2 PARTITION (part = '1')
SELECT key, count(*) FROM src GROUP BY key;

DESC FORMATTED list_bucketing_table2 PARTITION (part = '1');
