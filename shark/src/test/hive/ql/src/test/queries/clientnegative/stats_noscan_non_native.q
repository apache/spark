
CREATE TABLE non_native1(key int, value string) 
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler';

-- we do not support analyze table ... noscan on non-native tables yet
analyze table non_native1 compute statistics noscan;
