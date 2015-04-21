
select count(*) 
from src 
where src.key in (select key from src s1 where s1.key > '9') or src.value is not null
;