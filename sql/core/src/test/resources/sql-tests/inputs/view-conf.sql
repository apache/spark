CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
('a', 1), ('a', 2), ('b', 1), ('b', 2), ('c', 1), ('c', 2), (null, 1), ('c', null), (null, null)
AS testData(ColumnA, ColumnB);

-- ANSI
SET spark.sql.ansi.enabled = false;

CREATE TEMPORARY VIEW view_ansi AS SELECT ColumnA + 1 FROM testData;

SET spark.sql.ansi.enabled = true;

SELECT * FROM view_ansi;

-- Case sensitivity
SET spark.sql.caseSensitive = false;

CREATE TEMPORARY VIEW view_case_sensitivity AS SELECT CoLuMNa FROM testData;

SET spark.sql.caseSensitive = true;

SELECT * FROM view_case_sensitivity;

-- Clean up
DROP VIEW IF EXISTS view_ansi;
DROP VIEW IF EXISTS view_case_sensitivity;
