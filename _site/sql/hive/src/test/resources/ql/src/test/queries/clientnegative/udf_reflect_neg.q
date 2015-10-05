SELECT reflect("java.lang.StringClassThatDoesNotExist", "valueOf", 1),
       reflect("java.lang.String", "methodThatDoesNotExist"),
       reflect("java.lang.Math", "max", "overloadthatdoesnotexist", 3),
       reflect("java.lang.Math", "min", 2, 3),
       reflect("java.lang.Math", "round", 2.5),
       reflect("java.lang.Math", "exp", 1.0),
       reflect("java.lang.Math", "floor", 1.9)
FROM src LIMIT 1;

