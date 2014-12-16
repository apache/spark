set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;
set role ADMIN;
-- this is applicable to any security mode as check is in metastore
create role role1;
create role role2;
grant role role1 to role role2;

-- this will create a cycle
grant role role2 to role role1;
