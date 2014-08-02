DESCRIBE FUNCTION struct;
DESCRIBE FUNCTION EXTENDED struct;

EXPLAIN
SELECT struct(1), struct(1, "a"), struct(1, "b", 1.5).col1, struct(1, struct("a", 1.5)).col2.col1
FROM src LIMIT 1;

SELECT struct(1), struct(1, "a"), struct(1, "b", 1.5).col1, struct(1, struct("a", 1.5)).col2.col1
FROM src LIMIT 1;
