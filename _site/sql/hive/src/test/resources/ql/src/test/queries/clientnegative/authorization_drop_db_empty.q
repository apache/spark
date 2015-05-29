set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

-- check if changing owner and dropping as other user works
create database dba1;

set user.name=hive_admin_user;
set role ADMIN;
alter database dba1 set owner user user2;

set user.name=user2;
show current roles;
drop database dba1;


set user.name=user1;
-- check if dropping db as another user fails
show current roles;
create database dba2;

set user.name=user2;
show current roles;

drop database dba2;
