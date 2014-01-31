DESCRIBE FUNCTION elt;
DESCRIBE FUNCTION EXTENDED elt;

EXPLAIN
SELECT elt(2, 'abc', 'defg'),
       elt(3, 'aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg'),
       elt('1', 'abc', 'defg'),
       elt(2, 'aa', CAST('2' AS TINYINT)),
       elt(2, 'aa', CAST('12345' AS SMALLINT)),
       elt(2, 'aa', CAST('123456789012' AS BIGINT)),
       elt(2, 'aa', CAST(1.25 AS FLOAT)),
       elt(2, 'aa', CAST(16.0 AS DOUBLE)),
       elt(null, 'abc', 'defg'),
       elt(0, 'abc', 'defg'),
       elt(3, 'abc', 'defg')
FROM src LIMIT 1;

SELECT elt(2, 'abc', 'defg'),
       elt(3, 'aa', 'bb', 'cc', 'dd', 'ee', 'ff', 'gg'),
       elt('1', 'abc', 'defg'),
       elt(2, 'aa', CAST('2' AS TINYINT)),
       elt(2, 'aa', CAST('12345' AS SMALLINT)),
       elt(2, 'aa', CAST('123456789012' AS BIGINT)),
       elt(2, 'aa', CAST(1.25 AS FLOAT)),
       elt(2, 'aa', CAST(16.0 AS DOUBLE)),
       elt(null, 'abc', 'defg'),
       elt(0, 'abc', 'defg'),
       elt(3, 'abc', 'defg')
FROM src LIMIT 1;
