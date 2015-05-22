
set hive.compat=latest;

-- With ansi sql arithmetic enabled, int / int => exact numeric type
explain select cast(key as int) / cast(key as int) from src limit 1;
select cast(key as int) / cast(key as int) from src limit 1;


set hive.compat=0.12;

-- With ansi sql arithmetic disabled, int / int => double
explain select cast(key as int) / cast(key as int) from src limit 1;
select cast(key as int) / cast(key as int) from src limit 1;
