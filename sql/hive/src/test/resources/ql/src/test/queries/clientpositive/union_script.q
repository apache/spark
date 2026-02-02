select * from (
  select transform(key) using 'cat' as cola from src)s order by cola;

select * from (
  select transform(key) using 'cat' as cola from src
  union all
  select transform(key) using 'cat' as cola from src) s order by cola;
