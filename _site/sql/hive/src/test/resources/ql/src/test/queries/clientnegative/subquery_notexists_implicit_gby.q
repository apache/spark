

select * 
from src b 
where not exists 
  (select sum(1)
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9'
  )
;