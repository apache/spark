SET hive.metastore.partition.name.whitelist.pattern=[^9]*;
set hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.VerifyTableDirectoryIsEmptyHook;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create table source_table like srcpart;

create table dest_table like srcpart;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE source_table partition(ds='2008-04-08', hr=11);

-- Tests creating dynamic partitions with characters not in the whitelist (i.e. 9)
-- If the directory is not empty the hook will throw an error, instead the error should come from the metastore
-- This shows that no dynamic partitions were created and left behind or had directories created

insert overwrite table dest_table partition (ds, hr) select key, hr, ds, value from source_table where ds='2008-04-08' order by value asc;
