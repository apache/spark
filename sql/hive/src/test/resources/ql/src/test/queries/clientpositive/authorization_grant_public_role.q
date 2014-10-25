set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
-- current user has been set (comment line before the set cmd is resulting in parse error!!)

CREATE TABLE  t_gpr1(i int);

-- all privileges should have been set for user

GRANT ALL ON t_gpr1 TO ROLE public;

SHOW GRANT ON TABLE t_gpr1;

set user.name=user2;
SHOW CURRENT ROLES;
-- user2 should be able to do a describe table, as pubic is in the current roles
DESC t_gpr1;
