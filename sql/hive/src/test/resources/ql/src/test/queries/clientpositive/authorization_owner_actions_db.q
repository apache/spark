set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=hive_admin_user;

set role admin;
-- create role, db, make role the owner of db
create role testrole;
grant role testrole to user hrt_1;
create database testdb;
alter database testdb set owner role testrole;
desc database testdb;

-- actions that require user to be db owner 
-- create table
use testdb;
create table foobar (foo string, bar string);

-- drop db
drop database testdb cascade;
