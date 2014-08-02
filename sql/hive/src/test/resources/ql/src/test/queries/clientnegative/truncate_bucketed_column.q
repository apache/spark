-- Tests truncating a bucketed column

CREATE TABLE test_tab (key STRING, value STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS RCFILE;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src;

TRUNCATE TABLE test_tab COLUMNS (key);
