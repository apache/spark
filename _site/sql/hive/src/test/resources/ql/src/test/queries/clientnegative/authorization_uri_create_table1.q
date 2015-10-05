set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_crtab1;
dfs -touchz ${system:test.tmp.dir}/a_uri_crtab1/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/a_uri_crtab1/1.txt;

create table t1(i int) location '${system:test.tmp.dir}/a_uri_crtab_ext';

-- Attempt to create table with dir that does not have write permission should fail
