set hive.fetch.task.conversion=more;

-- casting from null should yield null
select
  cast(null as tinyint),
  cast(null as smallint),
  cast(null as int),
  cast(null as bigint),
  cast(null as float),
  cast(null as double),
  cast(null as decimal),
  cast(null as date),
  cast(null as timestamp),
  cast(null as string),
  cast(null as varchar(10)),
  cast(null as boolean),
  cast(null as binary)
from src limit 1;

-- Invalid conversions, should all be null
select
  cast('abcd' as date),
  cast('abcd' as timestamp)
from src limit 1;

