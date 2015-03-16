set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;
set role ADMIN;

create role testrole;
show roles;
drop role TESTROLE;
show roles;
create role TESTROLE;
show roles;
grant role testROLE to user hive_admin_user;
set role testrolE;
set role adMin;
show roles;
create role TESTRoLE;
