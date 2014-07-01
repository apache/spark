USE default;

-- Test of hive.exec.max.dynamic.partitions.pernode

CREATE TABLE max_parts(key STRING) PARTITIONED BY (value STRING);

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=10;

INSERT OVERWRITE TABLE max_parts PARTITION(value)
SELECT key, value
FROM src
LIMIT 50;
