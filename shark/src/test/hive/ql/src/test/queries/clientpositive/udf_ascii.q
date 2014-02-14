DESCRIBE FUNCTION ascii;
DESCRIBE FUNCTION EXTENDED ascii;

EXPLAIN SELECT
  ascii('Facebook'),
  ascii(''),
  ascii('!')
FROM src LIMIT 1;

SELECT
  ascii('Facebook'),
  ascii(''),
  ascii('!')
FROM src LIMIT 1;
