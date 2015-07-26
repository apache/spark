set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE table_priv_rfail1(i int);

-- grant insert to user2
GRANT INSERT ON table_priv_rfail1 TO USER user2;

set user.name=user3;
-- try dropping the privilege as user3
REVOKE INSERT ON TABLE table_priv_rfail1 FROM USER user2;
