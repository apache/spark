set hive.fetch.task.conversion=more;

-- test for TINYINT
select round(-128), round(127), round(0) from src tablesample (1 rows);

-- test for SMALLINT
select round(-32768), round(32767), round(-129), round(128) from src tablesample (1 rows);

-- test for INT
select round(cast(negative(pow(2, 31)) as INT)), round(cast((pow(2, 31) - 1) as INT)), round(-32769), round(32768) from src tablesample (1 rows);

-- test for BIGINT
select round(cast(negative(pow(2, 63)) as BIGINT)), round(cast((pow(2, 63) - 1) as BIGINT)), round(cast(negative(pow(2, 31) + 1) as BIGINT)), round(cast(pow(2, 31) as BIGINT)) from src tablesample (1 rows);

-- test for DOUBLE
select round(126.1), round(126.7), round(32766.1), round(32766.7) from src tablesample (1 rows);
