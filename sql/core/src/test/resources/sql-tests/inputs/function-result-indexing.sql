-- Indexing a result of a function that returns a complex type
SELECT split(col1, 'X')[0] AS a FROM VALUES('a') AS t(col1) ORDER BY split(col1, 'X')[0];
