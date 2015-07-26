set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE table_priv_gfail1(i int);

-- grant insert to user2 WITHOUT grant option
GRANT INSERT ON table_priv_gfail1 TO USER user2;

set user.name=user2;
-- try grant insert to user3
GRANT INSERT ON table_priv_gfail1 TO USER user3;
