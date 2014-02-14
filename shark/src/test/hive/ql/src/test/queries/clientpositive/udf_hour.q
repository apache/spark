DESCRIBE FUNCTION hour;
DESCRIBE FUNCTION EXTENDED hour;

EXPLAIN
SELECT hour('2009-08-07 13:14:15'), hour('13:14:15'), hour('2009-08-07')
FROM src WHERE key = 86;

SELECT hour('2009-08-07 13:14:15'), hour('13:14:15'), hour('2009-08-07')
FROM src WHERE key = 86;


SELECT hour(cast('2009-08-07 13:14:15'  as timestamp))
FROM src WHERE key=86;
