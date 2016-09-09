from src
select transform('aa\;') using 'cat' as a  limit 1;

from src
select transform('bb') using 'cat' as b limit 1; from src
select transform('cc') using 'cat' as c limit 1;