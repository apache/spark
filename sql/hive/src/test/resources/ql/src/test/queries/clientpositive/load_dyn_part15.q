
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table if not exists load_dyn_part15_test (key string) 
  partitioned by (part_key string);

show partitions load_dyn_part15_test;

INSERT OVERWRITE TABLE load_dyn_part15_test PARTITION(part_key)
SELECT key, part_key FROM src LATERAL VIEW explode(array("1","{2","3]")) myTable AS part_key;

show partitions load_dyn_part15_test;