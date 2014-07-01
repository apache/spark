DESCRIBE FUNCTION minute;
DESCRIBE FUNCTION EXTENDED minute;

EXPLAIN
SELECT minute('2009-08-07 13:14:15'), minute('13:14:15'), minute('2009-08-07')
FROM src WHERE key = 86;

SELECT minute('2009-08-07 13:14:15'), minute('13:14:15'), minute('2009-08-07')
FROM src WHERE key = 86;
