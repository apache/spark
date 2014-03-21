SET hive.metastore.partition.name.whitelist.pattern=[\\x20-\\x7E&&[^,]]* ;
-- This pattern matches all printable ASCII characters (disallow unicode) and disallows commas

CREATE TABLE part_whitelist_test (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS part_whitelist_test;

ALTER TABLE part_whitelist_test ADD PARTITION (ds='1,2,3,4');

