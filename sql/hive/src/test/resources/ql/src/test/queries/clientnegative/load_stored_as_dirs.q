set hive.mapred.supports.subdirectories=true;

-- Load data can't work with table with stored as directories
CREATE TABLE  if not exists stored_as_dirs_multiple (col1 STRING, col2 int, col3 STRING) 
SKEWED BY (col1, col2) ON (('s1',1), ('s3',3), ('s13',13), ('s78',78))  stored as DIRECTORIES;

LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE stored_as_dirs_multiple;
