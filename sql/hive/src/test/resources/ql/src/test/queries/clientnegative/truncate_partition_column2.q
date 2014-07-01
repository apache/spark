-- Tests truncating a partition column

CREATE TABLE test_tab (key STRING, value STRING) PARTITIONED BY (part STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE test_tab PARTITION (part = '1') SELECT * FROM src;

TRUNCATE TABLE test_tab PARTITION (part = '1') COLUMNS (part);
