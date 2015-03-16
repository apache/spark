set hive.fetch.task.conversion=more;

explain
select PI() FROM src tablesample (1 rows);

select PI() FROM src tablesample (1 rows);

DESCRIBE FUNCTION PI;
DESCRIBE FUNCTION EXTENDED PI;
explain 
select PI() FROM src tablesample (1 rows);

select PI() FROM src tablesample (1 rows);

DESCRIBE FUNCTION PI;
DESCRIBE FUNCTION EXTENDED PI;