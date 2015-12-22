set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE  tauth_gdup(i int);

-- It should be possible to revert owners privileges
revoke SELECT ON tauth_gdup from user user1;

show grant user user1 on table tauth_gdup;

-- Owner already has all privileges granted, another grant would become duplicate
-- and result in error
GRANT INSERT ON tauth_gdup TO USER user1;
