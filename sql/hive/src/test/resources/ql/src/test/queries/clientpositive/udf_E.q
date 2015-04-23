set hive.fetch.task.conversion=more;

explain
select E() FROM src tablesample (1 rows);

select E() FROM src tablesample (1 rows);

DESCRIBE FUNCTION E;
DESCRIBE FUNCTION EXTENDED E;
explain 
select E() FROM src tablesample (1 rows);

select E() FROM src tablesample (1 rows);

DESCRIBE FUNCTION E;
DESCRIBE FUNCTION EXTENDED E;
