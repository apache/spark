

-- no agg, corr
explain
select b.key, count(*) 
from src b 
group by b.key
having exists 
  (select a.key 
  from src a 
  where a.key = b.key and a.value > 'val_9'
  )
;

select b.key, count(*) 
from src b 
group by b.key
having exists 
  (select a.key 
  from src a 
  where a.key = b.key and a.value > 'val_9'
  )
;

-- view test
create view cv1 as 
select b.key, count(*) as c
from src b
group by b.key
having exists
  (select a.key
  from src a
  where a.key = b.key and a.value > 'val_9'
  )
;

select * from cv1;

-- sq in from
select *
from (select b.key, count(*) 
  from src b 
  group by b.key
  having exists 
    (select a.key 
    from src a 
    where a.key = b.key and a.value > 'val_9'
    )
) a
;

-- join on agg
select b.key, min(b.value)
from src b
group by b.key
having exists ( select a.key
                from src a
                where a.value > 'val_9' and a.value = min(b.value)
                )
;