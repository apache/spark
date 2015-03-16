
CREATE TABLE non_native2(key int, value string) 
STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler';

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE non_native2;
