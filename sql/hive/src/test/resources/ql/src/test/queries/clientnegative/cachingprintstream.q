set hive.exec.failure.hooks=org.apache.hadoop.hive.ql.hooks.VerifyCachingPrintStreamHook;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyCachingPrintStreamHook;

SELECT count(*) FROM src;
FROM src SELECT TRANSFORM (key, value) USING 'FAKE_SCRIPT_SHOULD_NOT_EXIST' AS key, value;

set hive.exec.failure.hooks=;
set hive.exec.post.hooks=;
