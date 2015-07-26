set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;
set role ADMIN;
show current roles;

create role r1;
grant role r1 to user hive_admin_user;
set role r1;
show current roles;

set role PUBLIC;
show current roles;

set role ALL;
show current roles;

set role ADMIN;
drop role r1;

