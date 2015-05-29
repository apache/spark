set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

-- running a sql query to initialize the authorization - not needed in real HS2 mode
show tables;

add jar ${system:maven.local.repository}/org/apache/hive/hcatalog/hive-hcatalog-core/${system:hive.version}/hive-hcatalog-core-${system:hive.version}.jar;
