create table default_partition_name (key int, value string) partitioned by (ds string);

set hive.exec.default.partition.name='some_other_default_partition_name';

alter table default_partition_name add partition(ds='__HIVE_DEFAULT_PARTITION__');

show partitions default_partition_name;
