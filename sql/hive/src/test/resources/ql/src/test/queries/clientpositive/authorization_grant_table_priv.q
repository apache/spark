set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE  table_priv1(i int);

-- all privileges should have been set for user

-- grant insert privilege to another user
GRANT INSERT ON table_priv1 TO USER user2;
SHOW GRANT USER user2 ON TABLE table_priv1;

-- grant select privilege to another user with grant
GRANT SELECT ON table_priv1 TO USER user2 with grant option;
SHOW GRANT USER user2 ON TABLE table_priv1;

set user.name=user2;
-- change to other user - user2
-- grant permissions to another user as user2
GRANT SELECT ON table_priv1 TO USER user3 with grant option;
SHOW GRANT USER user3 ON TABLE table_priv1;

set user.name=user3;
-- change to other user - user3
-- grant permissions to another user as user3
GRANT SELECT ON table_priv1 TO USER user4 with grant option;
SHOW GRANT USER user4 ON TABLE table_priv1;

set user.name=user1;
-- switched back to table owner

-- grant all with grant to user22
GRANT ALL ON table_priv1 TO USER user22 with grant option;
SHOW GRANT USER user22 ON TABLE table_priv1;

set user.name=user22;

-- grant all without grant to user33
GRANT ALL ON table_priv1 TO USER user33 with grant option;
SHOW GRANT USER user33 ON TABLE table_priv1;

