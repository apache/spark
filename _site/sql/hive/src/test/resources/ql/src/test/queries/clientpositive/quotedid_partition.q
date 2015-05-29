
set hive.support.quoted.identifiers=column;


create table src_p(`x+1` string, `y&y` string) partitioned by (`!@#$%^&*()_q` string);
insert overwrite table src_p partition(`!@#$%^&*()_q`='a') select * from src;

show partitions src_p;

explain select `x+1`, `y&y`, `!@#$%^&*()_q` 
from src_p where `!@#$%^&*()_q` = 'a' and `x+1`='10'
group by `x+1`, `y&y`, `!@#$%^&*()_q` having `!@#$%^&*()_q` = 'a'
;

set hive.exec.dynamic.partition.mode=nonstrict
;

create table src_p2(`x+1` string) partitioned by (`!@#$%^&*()_q` string);

insert overwrite table src_p2 partition(`!@#$%^&*()_q`)
select key, value as `!@#$%^&*()_q` from src where key < '200'
;

show partitions src_p2;