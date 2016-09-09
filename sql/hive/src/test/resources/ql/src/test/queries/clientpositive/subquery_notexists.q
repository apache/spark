

-- no agg, corr
explain
select * 
from src b 
where not exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_2'
  )
;

select * 
from src b 
where not exists 
  (select a.key 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_2'
  )
;

-- distinct, corr
explain
select * 
from src b 
where not exists 
  (select distinct a.key 
  from src a 
  where b.value = a.value and a.value > 'val_2'
  )
;

select * 
from src b 
where not exists 
  (select a.key 
  from src a 
  where b.value = a.value and a.value > 'val_2'
  )
;