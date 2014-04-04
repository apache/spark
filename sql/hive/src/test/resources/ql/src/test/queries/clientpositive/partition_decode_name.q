create table sc as select * 
from (select '2011-01-11', '2011-01-11+14:18:26' from src limit 1 
      union all 
      select '2011-01-11', '2011-01-11+15:18:26' from src limit 1 
      union all 
      select '2011-01-11', '2011-01-11+16:18:26' from src limit 1 ) s;

create table sc_part (key string) partitioned by (ts string) stored as rcfile;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.decode.partition.name=false;
insert overwrite table sc_part partition(ts) select * from sc;
show partitions sc_part;
select count(*) from sc_part where ts is not null;

set hive.decode.partition.name=true;
insert overwrite table sc_part partition(ts) select * from sc;
show partitions sc_part;
select count(*) from sc_part where ts is not null;
