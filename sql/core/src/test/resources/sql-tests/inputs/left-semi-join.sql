-- left semi greater than predicate
SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.a >= y.a + 2;

-- left semi greater than predicate and equal operator #1
SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.b = y.b and x.a >= y.a + 2;

-- left semi greater than predicate and equal operator #2
SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.b = y.a and x.a >= y.b + 1;