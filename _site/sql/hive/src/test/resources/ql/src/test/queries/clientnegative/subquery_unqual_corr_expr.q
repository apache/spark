

select *
from src
where key in (select key from src)
;