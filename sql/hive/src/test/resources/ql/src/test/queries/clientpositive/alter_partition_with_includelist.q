SET hive.metastore.partition.name.whitelist.pattern=[A-Za-z]*;
-- This pattern matches only letters.

CREATE TABLE part_includelist_test (key STRING, value STRING) PARTITIONED BY (ds STRING);
SHOW PARTITIONS part_includelist_test;

ALTER TABLE part_includelist_test ADD PARTITION (ds='Part');

ALTER TABLE part_includelist_test PARTITION (ds='Part') rename to partition (ds='Apart');
