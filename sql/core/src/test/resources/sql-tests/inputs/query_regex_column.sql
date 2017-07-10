set spark.sql.parser.quotedRegexColumnNames=false;

CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, "1", "11"), (2, "2", "22"), (3, "3", "33"), (4, "4", "44"), (5, "5", "55"), (6, "6", "66")
AS testData(key, value1, value2);

CREATE OR REPLACE TEMPORARY VIEW testData2 AS SELECT * FROM VALUES
(1, 1, 1, 2), (1, 2, 1, 2), (2, 1, 2, 3), (2, 2, 2, 3), (3, 1, 3, 4), (3, 2, 3, 4)
AS testData2(A, B, c, d);

-- AnalysisException
SELECT `(a)?+.+` FROM testData2 WHERE a = 1;
SELECT t.`(a)?+.+` FROM testData2 t WHERE a = 1;
SELECT `(a|b)` FROM testData2 WHERE a = 2;
SELECT `(a|b)?+.+` FROM testData2 WHERE a = 2;
SELECT SUM(`(a|b)?+.+`) FROM testData2;
SELECT SUM(`(a)`) FROM testData2;

set spark.sql.parser.quotedRegexColumnNames=true;

-- Regex columns
SELECT `(a)?+.+` FROM testData2 WHERE a = 1;
SELECT `(A)?+.+` FROM testData2 WHERE a = 1;
SELECT t.`(a)?+.+` FROM testData2 t WHERE a = 1;
SELECT t.`(A)?+.+` FROM testData2 t WHERE a = 1;
SELECT `(a|B)` FROM testData2 WHERE a = 2;
SELECT `(A|b)` FROM testData2 WHERE a = 2;
SELECT `(a|B)?+.+` FROM testData2 WHERE a = 2;
SELECT `(A|b)?+.+` FROM testData2 WHERE a = 2;
SELECT `(e|f)` FROM testData2;
SELECT t.`(e|f)` FROM testData2 t;
SELECT p.`(KEY)?+.+`, b, testdata2.`(b)?+.+` FROM testData p join testData2 ON p.key = testData2.a WHERE key < 3;
SELECT p.`(key)?+.+`, b, testdata2.`(b)?+.+` FROM testData p join testData2 ON p.key = testData2.a WHERE key < 3;

set spark.sql.caseSensitive=true;

CREATE OR REPLACE TEMPORARY VIEW testdata3 AS SELECT * FROM VALUES
(0, 1), (1, 2), (2, 3), (3, 4)
AS testdata3(a, b);

-- Regex columns
SELECT `(A)?+.+` FROM testdata3;
SELECT `(a)?+.+` FROM testdata3;
SELECT `(A)?+.+` FROM testdata3 WHERE a > 1;
SELECT `(a)?+.+` FROM testdata3 where `a` > 1;
SELECT SUM(`a`) FROM testdata3;
SELECT SUM(`(a)`) FROM testdata3;
SELECT SUM(`(a)?+.+`) FROM testdata3;
SELECT SUM(a) FROM testdata3 GROUP BY `a`;
-- AnalysisException
SELECT SUM(a) FROM testdata3 GROUP BY `(a)`;
SELECT SUM(a) FROM testdata3 GROUP BY `(a)?+.+`;
