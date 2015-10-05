
-- Should all be true
select
  cast('abc' as char(10)) =  cast('abc' as char(10)),
  cast('abc' as char(10)) <= cast('abc' as char(10)),
  cast('abc' as char(10)) >= cast('abc' as char(10)),
  cast('abc' as char(10)) <  cast('abd' as char(10)),
  cast('abc' as char(10)) >  cast('abb' as char(10)),
  cast('abc' as char(10)) <> cast('abb' as char(10))
from src limit 1;

-- Different char lengths should still compare the same
select
  cast('abc' as char(10)) =  cast('abc' as char(3)),
  cast('abc' as char(10)) <= cast('abc' as char(3)),
  cast('abc' as char(10)) >= cast('abc' as char(3)),
  cast('abc' as char(10)) <  cast('abd' as char(3)),
  cast('abc' as char(10)) >  cast('abb' as char(3)),
  cast('abc' as char(10)) <> cast('abb' as char(3))
from src limit 1;

-- Should work with string types as well
select
  cast('abc' as char(10)) =  'abc',
  cast('abc' as char(10)) <= 'abc',
  cast('abc' as char(10)) >= 'abc',
  cast('abc' as char(10)) <  'abd',
  cast('abc' as char(10)) >  'abb',
  cast('abc' as char(10)) <> 'abb'
from src limit 1;

-- leading space is significant for char
select
  cast(' abc' as char(10)) <> cast('abc' as char(10))
from src limit 1;

-- trailing space is not significant for char
select
  cast('abc ' as char(10)) = cast('abc' as char(10))
from src limit 1;
