set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;

set role ADMIN;

----------------------------------------
-- role granting with admin option
-- since user2 doesn't have admin option for role_noadmin, last grant should fail
----------------------------------------

create role role_noadmin;
create role src_role_wadmin;
grant  src_role_wadmin to user user2 with admin option;
grant  role_noadmin to user user2;
show role grant user user2;


set user.name=user2;
set role role_noadmin;
grant  src_role_wadmin to user user3;
