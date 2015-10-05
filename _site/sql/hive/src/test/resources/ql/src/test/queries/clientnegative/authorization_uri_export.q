set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=export_auth_uri;


create table export_auth_uri ( dep_id int comment "department id")
	stored as textfile;

dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/export_auth_uri/temp;
dfs -rmr target/tmp/ql/test/data/exports/export_auth_uri;


dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/export_auth_uri/;
dfs -chmod 555 target/tmp/ql/test/data/exports/export_auth_uri;

export table export_auth_uri to 'ql/test/data/exports/export_auth_uri';

-- Attempt to export to location without sufficient permissions should fail
