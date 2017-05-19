CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(1, "1"), (2, "2"), (3, "3"), (4, "4"), (5, "5"), (6, "6")
AS testData(key, value);

CREATE OR REPLACE TEMPORARY VIEW testData2 AS SELECT * FROM VALUES
(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)
AS testData2(a, b);

-- AnalysisException
SELECT `(a)?+.+` FROM testData2 WHERE a = 1;

-- AnalysisException
SELECT t.`(a)?+.+` FROM testData2 t WHERE a = 1;

set spark.sql.parser.quotedRegexColumnNames=true;

-- Regex columns
SELECT `(a)?+.+` FROM testData2 WHERE a = 1;
SELECT t.`(a)?+.+` FROM testData2 t WHERE a = 1;
SELECT p.`(key)?+.+`, b, testdata2.`(b)?+.+` FROM testData p join testData2 ON p.key = testData2.a WHERE key < 3;

-- Clean-up
DROP VIEW IF EXISTS testData;
DROP VIEW IF EXISTS testData2;
