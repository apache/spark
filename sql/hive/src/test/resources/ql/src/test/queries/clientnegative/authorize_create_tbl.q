set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set hive.security.authorization.enabled=true;
set user.name=user33;
create database db23221;
use db23221;

set user.name=user44;
create table twew221(a string);
