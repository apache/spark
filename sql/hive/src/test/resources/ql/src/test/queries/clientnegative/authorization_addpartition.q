set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set user.name=user1;
-- check add partition without insert privilege
create table tpart(i int, j int) partitioned by (k string);         

set user.name=user2;
alter table tpart add partition (k = 'abc');
