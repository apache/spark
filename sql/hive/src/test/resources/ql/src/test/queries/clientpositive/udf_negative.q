set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION negative;
DESCRIBE FUNCTION EXTENDED negative;

-- synonym
DESCRIBE FUNCTION -;
DESCRIBE FUNCTION EXTENDED -;

select - null from src tablesample (1 rows);
select - cast(null as int) from src tablesample (1 rows);
select - cast(null as smallint) from src tablesample (1 rows);
select - cast(null as bigint) from src tablesample (1 rows);
select - cast(null as double) from src tablesample (1 rows);
select - cast(null as float) from src tablesample (1 rows);
