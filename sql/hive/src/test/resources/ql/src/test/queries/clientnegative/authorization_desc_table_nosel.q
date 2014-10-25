set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

-- check if alter table fails as different user
create table t1(i int);
desc t1;

grant all on table t1 to user user2;
revoke select on table t1 from user user2;

set user.name=user2;
desc t1;
