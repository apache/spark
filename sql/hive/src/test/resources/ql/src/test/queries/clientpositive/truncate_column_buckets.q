-- Tests truncating columns from a bucketed table, table should remain bucketed

CREATE TABLE test_tab (key STRING, value STRING) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS RCFILE;

set hive.enforce.bucketing=true;

INSERT OVERWRITE TABLE test_tab SELECT * FROM src;

-- Check how many rows there are in each bucket, there should be two rows
SELECT cnt FROM (
SELECT INPUT__FILE__NAME file_name, count(*) cnt FROM 
test_tab GROUP BY INPUT__FILE__NAME
ORDER BY file_name DESC)a;

-- Truncate a column on which the table is not bucketed
TRUNCATE TABLE test_tab COLUMNS (value);

-- Check how many rows there are in each bucket, this should produce the same rows as before
-- because truncate should not break bucketing
SELECT cnt FROM (
SELECT INPUT__FILE__NAME file_name, count(*) cnt FROM 
test_tab GROUP BY INPUT__FILE__NAME
ORDER BY file_name DESC)a;
