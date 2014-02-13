DESCRIBE FUNCTION array_contains;
DESCRIBE FUNCTION EXTENDED array_contains;

-- evalutes function for array of primitives
SELECT array_contains(array(1, 2, 3), 1) FROM src LIMIT 1;

-- evaluates function for nested arrays
SELECT array_contains(array(array(1,2), array(2,3), array(3,4)), array(1,2))
FROM src LIMIT 1;
