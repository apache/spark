set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

-- an error should be thrown if 'set role ' is done for role that does not exist

set role nosuchroleexists;

