set hive.auto.convert.join=true;

explain 
select s1.key as key, s1.value as value from src s1 join src s3 on s1.key=s3.key
UNION  ALL  
select s2.key as key, s2.value as value from src s2;

create table ut as
select s1.key as key, s1.value as value from src s1 join src s3 on s1.key=s3.key
UNION  ALL  
select s2.key as key, s2.value as value from src s2;

select * from ut order by key, value limit 20;
drop table ut;

set hive.auto.convert.join=false;

explain
with u as (select * from src union all select * from src)
select count(*) from (select u1.key as k1, u2.key as k2 from
u as u1 join u as u2 on (u1.key = u2.key)) a;

create table ut as
with u as (select * from src union all select * from src)
select count(*) as cnt from (select u1.key as k1, u2.key as k2 from
u as u1 join u as u2 on (u1.key = u2.key)) a;

select * from ut order by cnt limit 20;
drop table ut;

set hive.auto.convert.join=true;

explain select s1.key as skey, u1.key as ukey from
src s1
join (select * from src union all select * from src) u1 on s1.key = u1.key;

create table ut as
select s1.key as skey, u1.key as ukey from
src s1
join (select * from src union all select * from src) u1 on s1.key = u1.key;

select * from ut order by skey, ukey limit 20;
drop table ut;

explain select s1.key as skey, u1.key as ukey, s8.key as lkey from 
src s1
join (select s2.key as key from src s2 join src s3 on s2.key = s3.key
      union all select s4.key from src s4 join src s5 on s4.key = s5.key
      union all select s6.key from src s6 join src s7 on s6.key = s7.key) u1 on (s1.key = u1.key)
join src s8 on (u1.key = s8.key)
order by lkey;

create table ut as
select s1.key as skey, u1.key as ukey, s8.key as lkey from 
src s1
join (select s2.key as key from src s2 join src s3 on s2.key = s3.key
      union all select s4.key from src s4 join src s5 on s4.key = s5.key
      union all select s6.key from src s6 join src s7 on s6.key = s7.key) u1 on (s1.key = u1.key)
join src s8 on (u1.key = s8.key)
order by lkey;

select * from ut order by skey, ukey, lkey limit 100;

drop table ut;

explain
select s2.key as key from src s2 join src s3 on s2.key = s3.key
union all select s4.key from src s4 join src s5 on s4.key = s5.key;

create table ut as
select s2.key as key from src s2 join src s3 on s2.key = s3.key
union all select s4.key from src s4 join src s5 on s4.key = s5.key;

select * from ut order by key limit 30;

drop table ut;

explain
select * from
(select * from src union all select * from src) u
left outer join src s on u.key = s.key;

explain
select u.key as ukey, s.key as skey from
(select * from src union all select * from src) u
right outer join src s on u.key = s.key;

create table ut as
select u.key as ukey, s.key as skey from
(select * from src union all select * from src) u
right outer join src s on u.key = s.key;

select * from ut order by ukey, skey limit 20;
drop table ut;