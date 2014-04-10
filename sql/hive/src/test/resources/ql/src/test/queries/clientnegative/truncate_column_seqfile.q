-- Tests truncating a column from a table stored as a sequence file

CREATE TABLE test_tab (key STRING, value STRING) STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src;

TRUNCATE TABLE test_tab COLUMNS (key);
