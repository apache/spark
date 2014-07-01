DESCRIBE FUNCTION create_union;
DESCRIBE FUNCTION EXTENDED create_union;

EXPLAIN
SELECT create_union(0, key), create_union(if(key<100, 0, 1), 2.0, value),
create_union(1, "a", struct(2, "b"))
FROM src LIMIT 2;

SELECT create_union(0, key), create_union(if(key<100, 0, 1), 2.0, value),
create_union(1, "a", struct(2, "b"))
FROM src LIMIT 2;
