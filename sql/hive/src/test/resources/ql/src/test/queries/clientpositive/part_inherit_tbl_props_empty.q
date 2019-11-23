set hive.metastore.partition.inherit.table.properties="";
create table mytbl (c1 tinyint) partitioned by (c2 string) tblproperties ('a'='myval','b'='yourval','c'='noval');
alter table mytbl add partition (c2 = 'v1');
describe formatted mytbl partition (c2='v1');
