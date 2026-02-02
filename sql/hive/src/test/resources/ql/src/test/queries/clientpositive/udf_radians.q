set hive.fetch.task.conversion=more;

explain
select radians(57.2958) FROM src tablesample (1 rows);

select radians(57.2958) FROM src tablesample (1 rows);
select radians(143.2394) FROM src tablesample (1 rows);

DESCRIBE FUNCTION radians;
DESCRIBE FUNCTION EXTENDED radians;
explain 
select radians(57.2958) FROM src tablesample (1 rows);

select radians(57.2958) FROM src tablesample (1 rows);
select radians(143.2394) FROM src tablesample (1 rows);

DESCRIBE FUNCTION radians;
DESCRIBE FUNCTION EXTENDED radians;