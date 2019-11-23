SET hive.metastore.partition.name.whitelist.pattern=;
-- Test with no partition name whitelist pattern

CREATE TABLE part_nowhitelist_test (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS part_nowhitelist_test;

ALTER TABLE part_nowhitelist_test ADD PARTITION (ds='1,2,3,4');
