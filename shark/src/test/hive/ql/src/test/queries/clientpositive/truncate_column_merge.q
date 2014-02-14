-- Tests truncating a column from a table with multiple files, then merging those files

CREATE TABLE test_tab (key STRING, value STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src LIMIT 5;

INSERT INTO TABLE test_tab SELECT * FROM src LIMIT 5;

-- The value should be 2 indicating the table has 2 files
SELECT COUNT(DISTINCT INPUT__FILE__NAME) FROM test_tab;

TRUNCATE TABLE test_tab COLUMNS (key);

ALTER TABLE test_tab CONCATENATE;

-- The first column (key) should be null for all 10 rows
SELECT * FROM test_tab ORDER BY value;

-- The value should be 1 indicating the table has 1 file
SELECT COUNT(DISTINCT INPUT__FILE__NAME) FROM test_tab;
