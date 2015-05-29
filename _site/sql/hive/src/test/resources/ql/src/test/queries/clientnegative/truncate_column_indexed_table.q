-- Tests truncating a column from an indexed table

CREATE TABLE test_tab (key STRING, value STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src;

CREATE INDEX test_tab_index ON TABLE test_tab (key) as 'COMPACT' WITH DEFERRED REBUILD;

TRUNCATE TABLE test_tab COLUMNS (value);
