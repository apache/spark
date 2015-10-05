set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=hive_admin_user;
set role ADMIN;
-- this is applicable to any security mode as check is in metastore

create role role1;

create role role2;
grant role role2 to role role1;

create role role3;
grant role role3 to role role2;

create role role4;
grant role role4 to role role3;

create role role5;
grant role role5 to role role4;

-- this will create a cycle in middle of the hierarchy
grant role role2 to role role4;
