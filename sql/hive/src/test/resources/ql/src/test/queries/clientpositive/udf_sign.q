set hive.fetch.task.conversion=more;

explain
select sign(0) FROM src tablesample (1 rows);
select sign(0) FROM src tablesample (1 rows);

select sign(-45) FROM src tablesample (1 rows);

select sign(46)  FROM src tablesample (1 rows);

DESCRIBE FUNCTION sign;
DESCRIBE FUNCTION EXTENDED sign;
explain 
select sign(0) FROM src tablesample (1 rows);
select sign(0) FROM src tablesample (1 rows);

select sign(-45) FROM src tablesample (1 rows);

select sign(46)  FROM src tablesample (1 rows);

DESCRIBE FUNCTION sign;
DESCRIBE FUNCTION EXTENDED sign;
