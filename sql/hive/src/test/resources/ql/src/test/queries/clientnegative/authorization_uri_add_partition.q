set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_uri_add_part;
dfs -touchz ${system:test.tmp.dir}/a_uri_add_part/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/a_uri_add_part/1.txt;

create table tpart(i int, j int) partitioned by (k string);
alter table tpart add partition (k = 'abc') location '${system:test.tmp.dir}/a_uri_add_part/';
