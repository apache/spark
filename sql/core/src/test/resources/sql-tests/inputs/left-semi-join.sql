-- A data set containing duplicate rows
CREATE OR REPLACE TEMPORARY VIEW duplicateColumnValueData AS SELECT * FROM VALUES
(1, 1),
(1, 2),
(2, 1),
(2, 2),
(3, 1),
(3, 2)
as duplicateRowData(a, b);

-- left semi greater than predicate
SELECT *
FROM duplicateColumnValueData x LEFT SEMI JOIN duplicateColumnValueData y
ON x.a >= y.a + 2;

-- left semi greater than predicate and equal operator #1
SELECT *
FROM duplicateColumnValueData x LEFT SEMI JOIN duplicateColumnValueData y
ON x.b = y.b and x.a >= y.a + 2;

-- left semi greater than predicate and equal operator #2
SELECT *
FROM duplicateColumnValueData x LEFT SEMI JOIN duplicateColumnValueData y
ON x.b = y.a and x.a >= y.b + 1;
