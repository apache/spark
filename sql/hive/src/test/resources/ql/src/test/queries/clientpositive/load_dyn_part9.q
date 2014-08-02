show partitions srcpart;



create table if not exists nzhang_part9 like srcpart;
describe extended nzhang_part9;

set hive.merge.mapfiles=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
from srcpart
insert overwrite table nzhang_part9 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08';

from srcpart
insert overwrite table nzhang_part9 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08';


show partitions nzhang_part9;

select * from nzhang_part9 where ds is not null and hr is not null;

