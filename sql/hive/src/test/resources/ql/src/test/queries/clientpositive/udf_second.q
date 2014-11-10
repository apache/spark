set hive.fetch.task.conversion=more;

DESCRIBE FUNCTION second;
DESCRIBE FUNCTION EXTENDED second;

EXPLAIN
SELECT second('2009-08-07 13:14:15'), second('13:14:15'), second('2009-08-07')
FROM src WHERE key = 86;

SELECT second('2009-08-07 13:14:15'), second('13:14:15'), second('2009-08-07')
FROM src WHERE key = 86;
