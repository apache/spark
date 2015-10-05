set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/az_uri_altertab_setloc;
dfs -touchz ${system:test.tmp.dir}/az_uri_altertab_setloc/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/az_uri_altertab_setloc/1.txt;

create table t1(i int);

alter table t1 set location '${system:test.tmp.dir}/az_uri_altertab_setloc/1.txt'

-- Attempt to set location of table to a location without permissions should fail
