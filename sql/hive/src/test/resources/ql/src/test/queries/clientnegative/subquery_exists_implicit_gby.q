

select * 
from src b 
where exists 
  (select count(*) 
  from src a 
  where b.value = a.value  and a.key = b.key and a.value > 'val_9'
  )
;