set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

-- check if alter table fails as different user
create table t1(i int);

set user.name=user2;
ALTER TABLE t1 SET SERDEPROPERTIES ('field.delim' = ',');
