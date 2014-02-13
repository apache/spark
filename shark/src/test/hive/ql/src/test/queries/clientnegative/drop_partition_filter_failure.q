create table ptestfilter1 (a string, b int) partitioned by (c string, d string);

alter table ptestfilter1 add partition (c='US', d=1);
show partitions ptestfilter1;

set hive.exec.drop.ignorenonexistent=false;
alter table ptestfilter1 drop partition (c='US', d<1);

