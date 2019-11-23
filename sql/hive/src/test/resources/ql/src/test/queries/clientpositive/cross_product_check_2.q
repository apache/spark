create table A as
select * from src;

create table B as
select * from src
limit 10;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000000;

explain select * from A join B;

explain select * from B d1 join B d2 on d1.key = d2.key join A;

explain select * from A join 
         (select d1.key 
          from B d1 join B d2 on d1.key = d2.key 
          where 1 = 1 group by d1.key) od1;
          
explain select * from A join (select d1.key from B d1 join B d2 where 1 = 1 group by d1.key) od1;

explain select * from 
(select A.key from A group by key) ss join 
(select d1.key from B d1 join B d2 on d1.key = d2.key where 1 = 1 group by d1.key) od1;


