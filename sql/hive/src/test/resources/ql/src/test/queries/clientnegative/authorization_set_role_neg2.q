set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_admin_user;
set role ADMIN;

-- an error should be thrown if 'set role ' is done for role that does not exist

create role rset_role_neg;
grant role rset_role_neg to user user2;

set user.name=user2;
set role rset_role_neg;
set role public;
set role nosuchroleexists;;

