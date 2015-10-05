USE default;

-- Test of hive.exec.max.dynamic.partitions
-- Set hive.exec.max.dynamic.partitions.pernode to a large value so it will be ignored

CREATE TABLE max_parts(key STRING) PARTITIONED BY (value STRING);

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10;
set hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE max_parts PARTITION(value)
SELECT key, value
FROM src
LIMIT 50;
