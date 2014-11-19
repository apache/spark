set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION ascii;
DESCRIBE FUNCTION EXTENDED ascii;

EXPLAIN SELECT
  ascii('Facebook'),
  ascii(''),
  ascii('!')
FROM src tablesample (1 rows);

SELECT
  ascii('Facebook'),
  ascii(''),
  ascii('!')
FROM src tablesample (1 rows);
