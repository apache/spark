set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/az_uri_alterpart_loc_perm;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/az_uri_alterpart_loc;
dfs -touchz ${system:test.tmp.dir}/az_uri_alterpart_loc/1.txt;
dfs -chmod 555 ${system:test.tmp.dir}/az_uri_alterpart_loc/1.txt;

create table tpart(i int, j int) partitioned by (k string);
alter table tpart add partition (k = 'abc') location '${system:test.tmp.dir}/az_uri_alterpart_loc_perm/';

alter table tpart partition (k = 'abc') set location '${system:test.tmp.dir}/az_uri_alterpart_loc/';


-- Attempt to set partition to location without permissions should fail
