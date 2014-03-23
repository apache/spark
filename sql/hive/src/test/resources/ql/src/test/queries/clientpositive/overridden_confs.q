set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.VerifyOverriddenConfigsHook;
set hive.config.doesnt.exit=abc;

select count(*) from src;
