DESCRIBE FUNCTION negative;
DESCRIBE FUNCTION EXTENDED negative;

-- synonym
DESCRIBE FUNCTION -;
DESCRIBE FUNCTION EXTENDED -;

select - null from src limit 1;
select - cast(null as int) from src limit 1;
select - cast(null as smallint) from src limit 1;
select - cast(null as bigint) from src limit 1;
select - cast(null as double) from src limit 1;
select - cast(null as float) from src limit 1;
