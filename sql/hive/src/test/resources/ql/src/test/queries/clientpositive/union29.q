create table union_subq_union(key int, value string);

explain
insert overwrite table union_subq_union 
select * from (
  select key, value from src 
  union all 
  select key, value from 
  (
    select key, value from src 
    union all
    select key, value from src
  ) subq
) a
;

insert overwrite table union_subq_union 
select * from (
  select key, value from src 
  union all 
  select key, value from 
  (
    select key, value from src 
    union all
    select key, value from src
  ) subq
) a
;

select * from union_subq_union order by key, value limit 20;
