
set hive.support.quoted.identifiers=column;

-- basic
create table t1(`x+1` string, `y&y` string, `!@#$%^&*()_q` string);
describe t1;
select `x+1`, `y&y`, `!@#$%^&*()_q` from t1;
explain select `x+1`, `y&y`, `!@#$%^&*()_q` from t1;
explain select `x+1`, `y&y`, `!@#$%^&*()_q` from t1 where `!@#$%^&*()_q` = '1';
explain select `x+1`, `y&y`, `!@#$%^&*()_q` from t1 where `!@#$%^&*()_q` = '1' group by `x+1`, `y&y`, `!@#$%^&*()_q` having `!@#$%^&*()_q` = '1';
explain select `x+1`, `y&y`, `!@#$%^&*()_q`, rank() over(partition by `!@#$%^&*()_q` order by  `y&y`)  
from t1 where `!@#$%^&*()_q` = '1' group by `x+1`, `y&y`, `!@#$%^&*()_q` having `!@#$%^&*()_q` = '1';

-- case insensitive
explain select `X+1`, `Y&y`, `!@#$%^&*()_Q`, rank() over(partition by `!@#$%^&*()_q` order by  `y&y`)  
from t1 where `!@#$%^&*()_q` = '1' group by `x+1`, `y&Y`, `!@#$%^&*()_q` having `!@#$%^&*()_Q` = '1';


-- escaped back ticks
create table t4(`x+1``` string, `y&y` string);
describe t4;
insert into table t4 select * from src;
select `x+1```, `y&y`, rank() over(partition by `x+1``` order by  `y&y`)  
from t4 where `x+1``` = '10' group by `x+1```, `y&y` having `x+1``` = '10';

-- view
create view v1 as 
select `x+1```, `y&y`
from t4 where `x+1``` < '200';

select `x+1```, `y&y`, rank() over(partition by `x+1``` order by  `y&y`)
from v1
group by `x+1```, `y&y`
;