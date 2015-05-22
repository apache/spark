set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set user.name=hive_admin_user;
show current roles;
set role ADMIN;

----------
-- create the following user, role mapping
-- user1 -> role1 -> role2 -> role3
----------

create role role1;
grant role1 to user user1;

create role role2;
grant role2 to role role1;

create role role3;
grant role3 to role role2;


create table t1(i int);
grant select on t1 to role role3;

set user.name=user1;
show current roles;
select * from t1;

set user.name=hive_admin_user;
show current roles;
grant select on t1 to role role2;


set user.name=user1;
show current roles;
select * from t1;

set user.name=hive_admin_user;
set role ADMIN;
show current roles;
revoke select on table t1 from role role2;


create role role4;
grant role4 to user user1;
grant role3 to role role4;;

set user.name=user1;
show current roles;
select * from t1;

set user.name=hive_admin_user;
show current roles;
set role ADMIN;

-- Revoke role3 from hierarchy one at a time and check permissions
-- after revoking from both, select should fail
revoke role3 from role role2;

set user.name=user1;
show current roles;
select * from t1;

set user.name=hive_admin_user;
show current roles;
set role ADMIN;
revoke role3 from role role4;

set user.name=user1;
show current roles;
select * from t1;
