SET hive.exec.driver.run.hooks=org.apache.hadoop.hive.ql.hooks.DriverTestHook;

-- This query should appear in the Hive CLI output.
-- We test DriverTestHook, which does exactly that.
-- This should not break.
SELECT * FROM src LIMIT 1;
