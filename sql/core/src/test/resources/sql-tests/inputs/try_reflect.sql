-- positive
SELECT try_reflect("java.util.UUID", "fromString", "a5cf6c42-0c85-418f-af6c-3e4e5b1328f2");
SELECT try_reflect("java.lang.String", "valueOf", 1);
SELECT try_reflect("java.lang.Math", "max", 2, 3);
SELECT try_reflect("java.lang.Math", "min", 2, 3);
SELECT try_reflect("java.lang.Integer", "valueOf", "10", 16);

-- negative
SELECT try_reflect("java.util.UUID", "fromString", "b");
SELECT try_reflect("java.net.URLDecoder", "decode", "%");
SELECT try_reflect("java.wrongclass.Math", "max", 2, 3);
SELECT try_reflect("java.lang.Math", "wrongmethod", 2, 3);
SELECT try_reflect("java.lang.Math");
SELECT try_reflect("java.lang.Math", "round", 2.5);
SELECT try_reflect("java.lang.Object", "toString");
