-- Create a table with one column, write to it, then add an additional column
-- This can break reads

CREATE TABLE test_orc (key STRING)
STORED AS ORC;

INSERT OVERWRITE TABLE test_orc SELECT key FROM src LIMIT 5;

ALTER TABLE test_orc ADD COLUMNS (value STRING);

SELECT * FROM test_orc order by key;
