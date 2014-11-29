
explain
select * 
from src 
where src.key in (select key from src) in (select key from src)
;