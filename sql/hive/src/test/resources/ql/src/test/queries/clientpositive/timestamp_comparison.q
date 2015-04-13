set hive.fetch.task.conversion=more;

select cast('2011-05-06 07:08:09' as timestamp) >
  cast('2011-05-06 07:08:09' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09' as timestamp) <
  cast('2011-05-06 07:08:09' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09' as timestamp) = 
  cast('2011-05-06 07:08:09' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09' as timestamp) <>
  cast('2011-05-06 07:08:09' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09' as timestamp) >=
  cast('2011-05-06 07:08:09' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09' as timestamp) <=
  cast('2011-05-06 07:08:09' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09' as timestamp) >=
  cast('2011-05-06 07:08:09.1' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09' as timestamp) <
  cast('2011-05-06 07:08:09.1' as timestamp) from src limit 1;

select cast('2011-05-06 07:08:09.1000' as timestamp) =
  cast('2011-05-06 07:08:09.1' as timestamp) from src limit 1;

