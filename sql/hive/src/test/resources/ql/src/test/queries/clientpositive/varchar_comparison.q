
-- Should all be true
select
  cast('abc' as varchar(10)) =  cast('abc' as varchar(10)),
  cast('abc' as varchar(10)) <= cast('abc' as varchar(10)),
  cast('abc' as varchar(10)) >= cast('abc' as varchar(10)),
  cast('abc' as varchar(10)) <  cast('abd' as varchar(10)),
  cast('abc' as varchar(10)) >  cast('abb' as varchar(10)),
  cast('abc' as varchar(10)) <> cast('abb' as varchar(10))
from src limit 1;

-- Different varchar lengths should still compare the same
select
  cast('abc' as varchar(10)) =  cast('abc' as varchar(3)),
  cast('abc' as varchar(10)) <= cast('abc' as varchar(3)),
  cast('abc' as varchar(10)) >= cast('abc' as varchar(3)),
  cast('abc' as varchar(10)) <  cast('abd' as varchar(3)),
  cast('abc' as varchar(10)) >  cast('abb' as varchar(3)),
  cast('abc' as varchar(10)) <> cast('abb' as varchar(3))
from src limit 1;

-- Should work with string types as well
select
  cast('abc' as varchar(10)) =  'abc',
  cast('abc' as varchar(10)) <= 'abc',
  cast('abc' as varchar(10)) >= 'abc',
  cast('abc' as varchar(10)) <  'abd',
  cast('abc' as varchar(10)) >  'abb',
  cast('abc' as varchar(10)) <> 'abb'
from src limit 1;

-- leading space is significant for varchar
select
  cast(' abc' as varchar(10)) <> cast('abc' as varchar(10))
from src limit 1;

-- trailing space is significant for varchar
select
  cast('abc ' as varchar(10)) <> cast('abc' as varchar(10))
from src limit 1;
