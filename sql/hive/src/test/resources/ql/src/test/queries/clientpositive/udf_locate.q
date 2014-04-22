DESCRIBE FUNCTION locate;
DESCRIBE FUNCTION EXTENDED locate;

EXPLAIN
SELECT locate('abc', 'abcd'),
       locate('ccc', 'abcabc'),
       locate('23', 123),
       locate(23, 123),
       locate('abc', 'abcabc', 2),
       locate('abc', 'abcabc', '2'),
       locate(1, TRUE),
       locate(1, FALSE),
       locate(CAST('2' AS TINYINT), '12345'),
       locate('34', CAST('12345' AS SMALLINT)),
       locate('456', CAST('123456789012' AS BIGINT)),
       locate('.25', CAST(1.25 AS FLOAT)),
       locate('.0', CAST(16.0 AS DOUBLE)),
       locate(null, 'abc'),
       locate('abc', null),
       locate('abc', 'abcd', null),
       locate('abc', 'abcd', 'invalid number')
FROM src LIMIT 1;

SELECT locate('abc', 'abcd'),
       locate('ccc', 'abcabc'),
       locate('23', 123),
       locate(23, 123),
       locate('abc', 'abcabc', 2),
       locate('abc', 'abcabc', '2'),
       locate(1, TRUE),
       locate(1, FALSE),
       locate(CAST('2' AS TINYINT), '12345'),
       locate('34', CAST('12345' AS SMALLINT)),
       locate('456', CAST('123456789012' AS BIGINT)),
       locate('.25', CAST(1.25 AS FLOAT)),
       locate('.0', CAST(16.0 AS DOUBLE)),
       locate(null, 'abc'),
       locate('abc', null),
       locate('abc', 'abcd', null),
       locate('abc', 'abcd', 'invalid number')
FROM src LIMIT 1;
