set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/az_uri_insert;
dfs -touchz ${system:test.tmp.dir}/az_uri_insert/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/az_uri_insert/1.txt;

create table t1(i int, j int);

insert overwrite directory '${system:test.tmp.dir}/az_uri_insert/' select * from t1;

-- Attempt to insert into uri without permissions should fail

