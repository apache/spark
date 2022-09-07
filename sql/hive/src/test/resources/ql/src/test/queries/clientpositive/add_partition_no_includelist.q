SET hive.metastore.partition.name.whitelist.pattern=;
-- Test with no partition name include-list pattern

CREATE TABLE part_noincludelist_test (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS part_noincludelist_test;

ALTER TABLE part_noincludelist_test ADD PARTITION (ds='1,2,3,4');
