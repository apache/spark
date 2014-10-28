set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

-- actions that require user to be table owner
create table t1(i int);

ALTER TABLE t1 SET SERDEPROPERTIES ('field.delim' = ',');
drop table t1;

create table t1(i int);
create view vt1 as select * from t1;

drop view vt1;
alter table t1 rename to tnew1;
