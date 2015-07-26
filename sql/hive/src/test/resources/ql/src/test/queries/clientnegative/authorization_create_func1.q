set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=hive_test_user;

-- permanent function creation should fail for non-admin roles
create function perm_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';
