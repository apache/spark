DESCRIBE FUNCTION map;
DESCRIBE FUNCTION EXTENDED map;

EXPLAIN SELECT map(), map(1, "a", 2, "b", 3, "c"), map(1, 2, "a", "b"), 
map(1, "a", 2, "b", 3, "c")[2],  map(1, 2, "a", "b")["a"], map(1, array("a"))[1][0] FROM src LIMIT 1;

SELECT map(), map(1, "a", 2, "b", 3, "c"), map(1, 2, "a", "b"), 
map(1, "a", 2, "b", 3, "c")[2],  map(1, 2, "a", "b")["a"], map(1, array("a"))[1][0] FROM src LIMIT 1;
