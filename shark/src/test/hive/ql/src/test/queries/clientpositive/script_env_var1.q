-- Verifies that script operator ID environment variables have unique values
-- in each instance of the script operator.
SELECT count(1) FROM
( SELECT TRANSFORM('echo $HIVE_SCRIPT_OPERATOR_ID') USING 'bash' AS key FROM src LIMIT 1 UNION ALL
  SELECT TRANSFORM('echo $HIVE_SCRIPT_OPERATOR_ID') USING 'bash' AS key FROM src LIMIT 1 ) a GROUP BY key;
