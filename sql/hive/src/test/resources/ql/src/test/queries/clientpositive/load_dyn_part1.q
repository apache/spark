show partitions srcpart;




create table if not exists nzhang_part1 like srcpart;
create table if not exists nzhang_part2 like srcpart;
describe extended nzhang_part1;

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

explain
from srcpart
insert overwrite table nzhang_part1 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08'
insert overwrite table nzhang_part2 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';

from srcpart
insert overwrite table nzhang_part1 partition (ds, hr) select key, value, ds, hr where ds <= '2008-04-08'
insert overwrite table nzhang_part2 partition(ds='2008-12-31', hr) select key, value, hr where ds > '2008-04-08';


show partitions nzhang_part1;
show partitions nzhang_part2;

select * from nzhang_part1 where ds is not null and hr is not null order by ds, hr, key;
select * from nzhang_part2 where ds is not null and hr is not null order by ds, hr, key;



