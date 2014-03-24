desc function str_to_map;
desc function extended str_to_map;

explain select str_to_map('a=1,b=2,c=3',',','=')['a'] from src limit 3;
select str_to_map('a=1,b=2,c=3',',','=')['a'] from src limit 3;

explain select str_to_map('a:1,b:2,c:3') from src limit 3;
select str_to_map('a:1,b:2,c:3') from src limit 3;

explain select str_to_map('a:1,b:2,c:3',',',':') from src limit 3;
select str_to_map('a:1,b:2,c:3',',',':') from src limit 3;

explain select str_to_map(t.ss,',',':')['a']
from (select transform('a:1,b:2,c:3') using 'cat' as (ss) from src) t
limit 3;
select str_to_map(t.ss,',',':')['a']
from (select transform('a:1,b:2,c:3') using 'cat' as (ss) from src) t
limit 3;


drop table tbl_s2m;
create table tbl_s2m as select 'ABC=CC_333=444' as t from src limit 3;

select str_to_map(t,'_','=')['333'] from tbl_s2m;

drop table tbl_s2m;
