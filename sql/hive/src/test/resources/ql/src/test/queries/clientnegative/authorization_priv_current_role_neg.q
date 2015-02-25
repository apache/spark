set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;
set role ADMIN;

-- the test verifies that authorization is happening with privileges of the current roles

-- grant privileges with grant option for table to role2 
create role role2;
grant role role2 to user user2;
create table tpriv_current_role(i int);
grant all on table tpriv_current_role to role role2 with grant option;

set user.name=user2;
-- switch to user2

-- by default all roles should be in current roles, and grant to new user should work
show current roles;
grant all on table tpriv_current_role to user user3;

set role role2;
-- switch to role2, grant should work
grant all on table tpriv_current_role to user user4;
show grant user user4 on table tpriv_current_role;

set role PUBLIC;
-- set role to public, should fail as role2 is not one of the current roles
grant all on table tpriv_current_role to user user5;
