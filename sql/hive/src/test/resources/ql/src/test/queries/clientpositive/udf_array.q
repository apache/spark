DESCRIBE FUNCTION array;
DESCRIBE FUNCTION EXTENDED array;

EXPLAIN SELECT array(), array()[1], array(1, 2, 3), array(1, 2, 3)[2], array(1,"a", 2, 3), array(1,"a", 2, 3)[2],
array(array(1), array(2), array(3), array(4))[1][0] FROM src LIMIT 1;

SELECT array(), array()[1], array(1, 2, 3), array(1, 2, 3)[2], array(1,"a", 2, 3), array(1,"a", 2, 3)[2],
array(array(1), array(2), array(3), array(4))[1][0] FROM src LIMIT 1;
