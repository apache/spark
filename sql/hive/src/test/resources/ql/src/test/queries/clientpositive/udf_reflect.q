DESCRIBE FUNCTION reflect;
DESCRIBE FUNCTION EXTENDED reflect;

EXPLAIN EXTENDED
SELECT reflect("java.lang.String", "valueOf", 1),
       reflect("java.lang.String", "isEmpty"),
       reflect("java.lang.Math", "max", 2, 3),
       reflect("java.lang.Math", "min", 2, 3),
       reflect("java.lang.Math", "round", 2.5),
       reflect("java.lang.Math", "exp", 1.0),
       reflect("java.lang.Math", "floor", 1.9),
       reflect("java.lang.Integer", "valueOf", key, 16)
FROM src LIMIT 1;


SELECT reflect("java.lang.String", "valueOf", 1),
       reflect("java.lang.String", "isEmpty"),
       reflect("java.lang.Math", "max", 2, 3),
       reflect("java.lang.Math", "min", 2, 3),
       reflect("java.lang.Math", "round", 2.5),
       reflect("java.lang.Math", "exp", 1.0),
       reflect("java.lang.Math", "floor", 1.9),
       reflect("java.lang.Integer", "valueOf", key, 16)
FROM src LIMIT 1;
