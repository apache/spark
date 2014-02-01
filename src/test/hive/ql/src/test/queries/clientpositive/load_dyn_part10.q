show partitions srcpart;



create table if not exists nzhang_part10 like srcpart;
describe extended nzhang_part10;

set hive.merge.mapfiles=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
from srcpart
insert overwrite table nzhang_part10 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';

from srcpart
insert overwrite table nzhang_part10 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';


show partitions nzhang_part10;

select * from nzhang_part10 where ds is not null and hr is not null;


