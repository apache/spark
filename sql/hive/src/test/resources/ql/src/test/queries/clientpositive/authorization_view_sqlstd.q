set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;
set user.name=user1;

-- Test view authorization , and 'show grant' variants

create table t1(i int, j int, k int);
show grant on table t1;

-- protecting certain columns
create view vt1 as select i,k from t1;

-- protecting certain rows
create view vt2 as select * from t1 where i > 1;

show grant user user1 on all;

--view grant to user
-- try with and without table keyword

grant select on vt1 to user user2;
grant insert on table vt1 to user user3;

show grant user user2 on table vt1;
show grant user user3 on table vt1;


set user.name=user2;
select * from vt1;

set user.name=user1;

grant all on table vt2 to user user2;
show grant user user2 on table vt2;
show grant user user2 on all;

revoke all on vt2 from user user2;
show grant user user2 on table vt2;

show grant on table vt2;


revoke select on table vt1 from user user2;
show grant user user2 on table vt1;

show grant user user2 on all;

-- grant privileges on roles for view, after next statement
show grant user user3 on table vt1;

set user.name=hive_admin_user;
show current roles;
set role ADMIN;
create role role_v;
grant  role_v to user user4 ;
show role grant user user4;
show roles;

grant all on table vt2 to role role_v;
show grant role role_v on table vt2;

revoke delete on table vt2 from role role_v;
show grant role role_v on table vt2;
show grant on table vt2;
