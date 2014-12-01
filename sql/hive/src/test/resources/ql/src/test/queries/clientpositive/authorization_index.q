set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.stats.dbclass=fs;
set hive.security.authorization.enabled=true;
create table t1 (a int);
create index t1_index on table t1(a) as 'COMPACT' WITH DEFERRED REBUILD;
desc formatted default__t1_t1_index__;
alter index t1_index on t1 rebuild;

drop table t1;

set hive.security.authorization.enabled=false;
