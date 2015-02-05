set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=import_auth_uri;


create table import_auth_uri ( dep_id int comment "department id")
	stored as textfile;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/import_auth_uri/temp;
dfs -rmr target/tmp/ql/test/data/exports/import_auth_uri;
export table import_auth_uri to 'ql/test/data/exports/import_auth_uri';
drop table import_auth_uri;

dfs -touchz target/tmp/ql/test/data/exports/import_auth_uri/1.txt;
dfs -chmod 555 target/tmp/ql/test/data/exports/import_auth_uri/1.txt;

create database importer;
use importer;

import from 'ql/test/data/exports/import_auth_uri';

-- Attempt to import from location without sufficient permissions should fail
