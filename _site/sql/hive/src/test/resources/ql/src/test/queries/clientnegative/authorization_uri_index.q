set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/az_uri_index;
dfs -touchz ${system:test.tmp.dir}/az_uri_index/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/az_uri_index/1.txt;


create table t1(i int);
create index idt1 on table t1 (i) as 'COMPACT' WITH DEFERRED REBUILD LOCATION '${system:test.tmp.dir}/az_uri_index/';

-- Attempt to use location for index that does not have permissions should fail
