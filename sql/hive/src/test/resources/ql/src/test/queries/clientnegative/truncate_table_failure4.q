CREATE TABLE non_native(key int, value string)
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler';

-- trucate for non-native table
TRUNCATE TABLE non_native;
