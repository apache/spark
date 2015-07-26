set hive.fetch.task.conversion=more;

explain
select degrees(PI()) FROM src tablesample (1 rows);

select degrees(PI()) FROM src tablesample (1 rows);

DESCRIBE FUNCTION degrees;
DESCRIBE FUNCTION EXTENDED degrees;
explain 
select degrees(PI()) FROM src tablesample (1 rows);

select degrees(PI()) FROM src tablesample (1 rows);

DESCRIBE FUNCTION degrees;
DESCRIBE FUNCTION EXTENDED degrees;