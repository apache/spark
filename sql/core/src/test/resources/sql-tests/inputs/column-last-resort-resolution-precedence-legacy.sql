-- This is a legacy test for last resort column resolution that has a correctness issue.
-- For more information, see SPARK-53733.

--IMPORT column-last-resort-resolution-precedence.sql

--SET spark.sql.analyzer.delayLastResortColumnResolution = false
