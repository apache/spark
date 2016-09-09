set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=hive_admin_user;

-- admin required for create function
set role ADMIN;

create temporary function temp_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';
create function perm_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

drop temporary function temp_fn;
drop function perm_fn;
