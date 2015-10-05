-- Tests attempting to truncate a column in a table that doesn't exist

CREATE TABLE test_tab (key STRING, value STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src;

TRUNCATE TABLE test_tab COLUMNS (doesnt_exist);
