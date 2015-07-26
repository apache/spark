set hive.fetch.task.conversion=more;

-- Comparisons against same value
select cast('2011-05-06' as date) > 
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-06' as date) <
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-06' as date) = 
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-06' as date) <>
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-06' as date) >=
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-06' as date) <=
  cast('2011-05-06' as date) from src limit 1;

-- Now try with differing values
select cast('2011-05-05' as date) > 
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-05' as date) <
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-05' as date) = 
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-05' as date) <>
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-05' as date) >=
  cast('2011-05-06' as date) from src limit 1;

select cast('2011-05-05' as date) <=
  cast('2011-05-06' as date) from src limit 1;

