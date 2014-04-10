DESCRIBE FUNCTION concat;
DESCRIBE FUNCTION EXTENDED concat;

SELECT
  concat('a', 'b'),
  concat('a', 'b', 'c'),
  concat('a', null, 'c'),
  concat(null),
  concat('a'),
  concat(null, 1, 2),
  concat(1, 2, 3, 'a'),
  concat(1, 2),
  concat(1),
  concat('1234', 'abc', 'extra argument')
FROM src LIMIT 1;
