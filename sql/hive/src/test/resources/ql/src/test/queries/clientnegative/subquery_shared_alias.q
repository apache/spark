

select *
from src
where src.key in (select key from src where key > '9')
;