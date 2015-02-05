

-- no agg, corr
explain
select * 
from src b 
group by key, value
having not exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_12'
  )
;

select * 
from src b 
group by key, value
having not exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_12'
  )
;


-- distinct, corr
explain
select * 
from src b 
group by key, value
having not exists 
  (select distinct a.key 
  from src a 
  where b.value = a.value and a.value > 'val_12'
  )
;

select * 
from src b 
group by key, value
having not exists 
  (select distinct a.key 
  from src a 
  where b.value = a.value and a.value > 'val_12'
  )
;