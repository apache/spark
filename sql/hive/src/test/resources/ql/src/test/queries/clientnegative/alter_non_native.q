
CREATE TABLE non_native1(key int, value string) 
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler';

-- we do not support ALTER TABLE on non-native tables yet
ALTER TABLE non_native1 RENAME TO new_non_native;
