set hive.fetch.task.conversion=more;

-- cast string floats to integer types
select
  cast('1' as float),
  cast('1.4' as float),
  cast('1.6' as float),
  cast('1' as int),
  cast('1.4' as int),
  cast('1.6' as int),
  cast('1' as tinyint),
  cast('1.4' as tinyint),
  cast('1.6' as tinyint),
  cast('1' as smallint),
  cast('1.4' as smallint),
  cast('1.6' as smallint),
  cast('1' as bigint),
  cast('1.4' as bigint),
  cast('1.6' as bigint),
  cast (cast('1' as float) as int),
  cast(cast ('1.4' as float) as int),
  cast(cast ('1.6' as float) as int),
  cast('+1e5' as int),
  cast('2147483647' as int),
  cast('-2147483648' as int),
  cast('32767' as smallint),
  cast('-32768' as smallint),
  cast('-128' as tinyint),
  cast('127' as tinyint),
  cast('1.0a' as int),
  cast('-1.-1' as int)
from src tablesample (1 rows);
