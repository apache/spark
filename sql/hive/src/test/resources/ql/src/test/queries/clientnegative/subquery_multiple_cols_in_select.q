

explain
 select * 
from src 
where src.key in (select * from src s1 where s1.key > '9')
;