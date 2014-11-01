set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=hive_test_user;

-- temp function creation should fail for non-admin roles
create temporary function temp_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

