set hive.fetch.task.conversion=more;

select cast(cast('a' as binary) as string) from src tablesample (1 rows);
