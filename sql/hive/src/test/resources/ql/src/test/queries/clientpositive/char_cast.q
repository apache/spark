
-- Cast from char to other data types
select
  cast(cast('11' as string) as tinyint),
  cast(cast('11' as string) as smallint),
  cast(cast('11' as string) as int),
  cast(cast('11' as string) as bigint),
  cast(cast('11.00' as string) as float),
  cast(cast('11.00' as string) as double),
  cast(cast('11.00' as string) as decimal)
from src limit 1;

select
  cast(cast('11' as char(10)) as tinyint),
  cast(cast('11' as char(10)) as smallint),
  cast(cast('11' as char(10)) as int),
  cast(cast('11' as char(10)) as bigint),
  cast(cast('11.00' as char(10)) as float),
  cast(cast('11.00' as char(10)) as double),
  cast(cast('11.00' as char(10)) as decimal)
from src limit 1;

select
  cast(cast('2011-01-01' as string) as date),
  cast(cast('2011-01-01 01:02:03' as string) as timestamp)
from src limit 1;

select
  cast(cast('2011-01-01' as char(10)) as date),
  cast(cast('2011-01-01 01:02:03' as char(30)) as timestamp)
from src limit 1;

-- no tests from string/char to boolean, that conversion doesn't look useful
select
  cast(cast('abc123' as string) as string),
  cast(cast('abc123' as string) as varchar(10)),
  cast(cast('abc123' as string) as char(10))
from src limit 1;

select
  cast(cast('abc123' as char(10)) as string),
  cast(cast('abc123' as char(10)) as varchar(10)),
  cast(cast('abc123' as char(10)) as char(10))
from src limit 1;

select
  cast(cast('abc123' as varchar(10)) as string),
  cast(cast('abc123' as varchar(10)) as varchar(10)),
  cast(cast('abc123' as varchar(10)) as char(10))
from src limit 1;

-- cast from other types to char
select
  cast(cast(11 as tinyint) as string),
  cast(cast(11 as smallint) as string),
  cast(cast(11 as int) as string),
  cast(cast(11 as bigint) as string),
  cast(cast(11.00 as float) as string),
  cast(cast(11.00 as double) as string),
  cast(cast(11.00 as decimal) as string)
from src limit 1;

select
  cast(cast(11 as tinyint) as char(10)),
  cast(cast(11 as smallint) as char(10)),
  cast(cast(11 as int) as char(10)),
  cast(cast(11 as bigint) as char(10)),
  cast(cast(11.00 as float) as char(10)),
  cast(cast(11.00 as double) as char(10)),
  cast(cast(11.00 as decimal) as char(10))
from src limit 1;

select
  cast(date '2011-01-01' as string),
  cast(timestamp('2011-01-01 01:02:03') as string)
from src limit 1;

select
  cast(date '2011-01-01' as char(10)),
  cast(timestamp('2011-01-01 01:02:03') as char(30))
from src limit 1;

select
  cast(true as string),
  cast(false as string)
from src limit 1;

select
  cast(true as char(10)),
  cast(false as char(10))
from src limit 1;

