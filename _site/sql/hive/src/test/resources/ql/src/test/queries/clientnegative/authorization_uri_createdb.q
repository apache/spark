set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/az_uri_createdb;
dfs -touchz ${system:test.tmp.dir}/az_uri_createdb/1.txt;
dfs -chmod 300 ${system:test.tmp.dir}/az_uri_createdb/1.txt;

create database az_test_db location '${system:test.tmp.dir}/az_uri_createdb/';

-- Attempt to create db for dir without sufficient permissions should fail

