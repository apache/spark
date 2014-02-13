set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;

-- Tests truncating a column on which a table is list bucketed

CREATE TABLE test_tab (key STRING, value STRING) STORED AS RCFILE;

ALTER TABLE test_tab
SKEWED BY (key) ON ("484")
STORED AS DIRECTORIES;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src;

TRUNCATE TABLE test_tab COLUMNS (key);
