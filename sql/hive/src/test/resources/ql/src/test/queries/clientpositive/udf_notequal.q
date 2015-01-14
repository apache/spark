set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION <>;
DESCRIBE FUNCTION EXTENDED <>;

DESCRIBE FUNCTION !=;
DESCRIBE FUNCTION EXTENDED !=;

EXPLAIN
SELECT key, value
FROM src
WHERE key <> '302';

SELECT key, value
FROM src
WHERE key <> '302';

EXPLAIN
SELECT key, value
FROM src
WHERE key != '302';

SELECT key, value
FROM src
WHERE key != '302';
