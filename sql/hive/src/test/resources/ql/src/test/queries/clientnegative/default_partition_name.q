create table default_partition_name (key int, value string) partitioned by (ds string);

alter table default_partition_name add partition(ds='__HIVE_DEFAULT_PARTITION__');
