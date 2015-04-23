set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set hive.cli.print.header=true;
set user.name=hive_admin_user;
set role ADMIN;

----------------------------------------
-- role granting with admin option
----------------------------------------

create role src_role_wadmin;
grant  src_role_wadmin to user user2 with admin option;
show role grant user user2;
show principals src_role_wadmin;

set user.name=user2;
set role src_role_wadmin;
grant  src_role_wadmin to user user3;
show role grant user user3;

set user.name=hive_admin_user;
set role ADMIN;
show principals src_role_wadmin;

set user.name=user2;
set role src_role_wadmin;
revoke src_role_wadmin from user user3;
show role grant user user3;

set user.name=hive_admin_user;
set role ADMIN;
show principals src_role_wadmin;
