set hive.metastore.partition.inherit.table.properties=key1,*;
-- The property needs to be unset at the end of the test till HIVE-3109/HIVE-3112 is fixed

create table mytbl (c1 tinyint) partitioned by (c2 string) tblproperties ('a'='myval','b'='yourval','c'='noval');
alter table mytbl add partition (c2 = 'v1');
describe formatted mytbl partition (c2='v1');

set hive.metastore.partition.inherit.table.properties=;
