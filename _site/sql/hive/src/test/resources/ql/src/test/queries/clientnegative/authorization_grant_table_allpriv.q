set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE table_priv_allf(i int);

-- grant insert to user2 WITH grant option
GRANT INSERT ON table_priv_allf TO USER user2 with grant option;

set user.name=user2;
-- try grant all to user3, without having all privileges
GRANT ALL ON table_priv_allf TO USER user3;
