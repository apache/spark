explain
select s.key2, s.value2
from (
  select transform(key, value) using 'cat' as (key2, value2)
  from src
  union all 
  select key as key2, value as value2 from src) s
order by s.key2, s.value2;

select s.key2, s.value2
from (
  select transform(key, value) using 'cat' as (key2, value2)
  from src
  union all 
  select key as key2, value as value2 from src) s
order by s.key2, s.value2;

