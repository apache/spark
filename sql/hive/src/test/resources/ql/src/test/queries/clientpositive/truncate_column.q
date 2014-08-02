-- Tests truncating column(s) from a table, also tests that stats are updated

CREATE TABLE test_tab (key STRING, value STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE;

set hive.stats.autogather=true;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src LIMIT 10;

DESC FORMATTED test_tab;

SELECT * FROM test_tab ORDER BY value;

-- Truncate 1 column
TRUNCATE TABLE test_tab COLUMNS (key);

DESC FORMATTED test_tab;

-- First column should be null
SELECT * FROM test_tab ORDER BY value;

-- Truncate multiple columns
INSERT OVERWRITE TABLE test_tab SELECT * FROM src LIMIT 10;

TRUNCATE TABLE test_tab COLUMNS (key, value);

DESC FORMATTED test_tab;

-- Both columns should be null
SELECT * FROM test_tab ORDER BY value;

-- Truncate columns again
TRUNCATE TABLE test_tab COLUMNS (key, value);

DESC FORMATTED test_tab;

-- Both columns should be null
SELECT * FROM test_tab ORDER BY value;

-- Test truncating with a binary serde
ALTER TABLE test_tab SET SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

INSERT OVERWRITE TABLE test_tab SELECT * FROM src LIMIT 10;

DESC FORMATTED test_tab;

SELECT * FROM test_tab ORDER BY value;

-- Truncate 1 column
TRUNCATE TABLE test_tab COLUMNS (key);

DESC FORMATTED test_tab;

-- First column should be null
SELECT * FROM test_tab ORDER BY value;

-- Truncate 2 columns
TRUNCATE TABLE test_tab COLUMNS (key, value);

DESC FORMATTED test_tab;

-- Both columns should be null
SELECT * FROM test_tab ORDER BY value;

-- Test truncating a partition
CREATE TABLE test_tab_part (key STRING, value STRING) PARTITIONED BY (part STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE test_tab_part PARTITION (part = '1') SELECT * FROM src LIMIT 10;

DESC FORMATTED test_tab_part PARTITION (part = '1');

SELECT * FROM test_tab_part WHERE part = '1' ORDER BY value;

TRUNCATE TABLE test_tab_part PARTITION (part = '1') COLUMNS (key);

DESC FORMATTED test_tab_part PARTITION (part = '1');

-- First column should be null
SELECT * FROM test_tab_part WHERE part = '1' ORDER BY value;
