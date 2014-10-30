set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/authz_uri_load_data;
dfs -touchz ${system:test.tmp.dir}/authz_uri_load_data/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/authz_uri_load_data/1.txt;

create table t1(i int);
load data inpath 'pfile:${system:test.tmp.dir}/authz_uri_load_data/' overwrite into table t1;

