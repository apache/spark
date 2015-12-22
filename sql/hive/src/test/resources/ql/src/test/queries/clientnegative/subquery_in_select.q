


select src.key in (select key from src s1 where s1.key > '9') 
from src
;