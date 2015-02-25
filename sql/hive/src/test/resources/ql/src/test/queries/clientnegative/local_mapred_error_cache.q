set hive.exec.mode.local.auto=true;
set hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.VerifySessionStateLocalErrorsHook;

FROM src SELECT TRANSFORM(key, value) USING 'python ../../data/scripts/cat_error.py' AS (key, value);
