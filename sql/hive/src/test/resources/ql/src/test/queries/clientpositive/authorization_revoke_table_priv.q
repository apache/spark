set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE table_priv_rev(i int);

-- grant insert privilege to user2
GRANT INSERT ON table_priv_rev TO USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;
SHOW GRANT USER user2 ON ALL;

-- revoke insert privilege from user2
REVOKE INSERT ON TABLE table_priv_rev FROM USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;

-- grant all privileges one at a time --
-- grant insert privilege to user2
GRANT INSERT ON table_priv_rev TO USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;
SHOW GRANT USER user2 ON ALL;

-- grant select privilege to user2, with grant option
GRANT SELECT ON table_priv_rev TO USER user2 WITH GRANT OPTION;
SHOW GRANT USER user2 ON TABLE table_priv_rev;

-- grant update privilege to user2
GRANT UPDATE ON table_priv_rev TO USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;

-- grant delete privilege to user2
GRANT DELETE ON table_priv_rev TO USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;


-- start revoking --
-- revoke update privilege from user2
REVOKE UPDATE ON TABLE table_priv_rev FROM USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;
SHOW GRANT USER user2 ON ALL;

-- revoke DELETE privilege from user2
REVOKE DELETE ON TABLE table_priv_rev FROM USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;

-- revoke insert privilege from user2
REVOKE INSERT ON TABLE table_priv_rev FROM USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;

-- revoke select privilege from user2
REVOKE SELECT ON TABLE table_priv_rev FROM USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;
SHOW GRANT USER user2 ON ALL;

-- grant all followed by revoke all
GRANT ALL ON table_priv_rev TO USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;

REVOKE ALL ON TABLE table_priv_rev FROM USER user2;
SHOW GRANT USER user2 ON TABLE table_priv_rev;
