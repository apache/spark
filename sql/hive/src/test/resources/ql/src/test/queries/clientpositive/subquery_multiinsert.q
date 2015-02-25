set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.hadoop.hive.ql.hooks.PrintCompletedTasksHook;

CREATE TABLE src_4(
  key STRING, 
  value STRING
)
;

CREATE TABLE src_5( 
  key STRING, 
  value STRING
)
;

explain
from src b 
INSERT OVERWRITE TABLE src_4 
  select * 
  where b.key in 
   (select a.key 
    from src a 
    where b.value = a.value and a.key > '9'
   ) 
INSERT OVERWRITE TABLE src_5 
  select *  
  where b.key not in  ( select key from src s1 where s1.key > '2') 
  order by key 
;

from src b 
INSERT OVERWRITE TABLE src_4 
  select * 
  where b.key in 
   (select a.key 
    from src a 
    where b.value = a.value and a.key > '9'
   ) 
INSERT OVERWRITE TABLE src_5 
  select *  
  where b.key not in  ( select key from src s1 where s1.key > '2') 
  order by key 
;

select * from src_4
;
select * from src_5
;
set hive.auto.convert.join=true;

explain
from src b 
INSERT OVERWRITE TABLE src_4 
  select * 
  where b.key in 
   (select a.key 
    from src a 
    where b.value = a.value and a.key > '9'
   ) 
INSERT OVERWRITE TABLE src_5 
  select *  
  where b.key not in  ( select key from src s1 where s1.key > '2') 
  order by key 
;

from src b 
INSERT OVERWRITE TABLE src_4 
  select * 
  where b.key in 
   (select a.key 
    from src a 
    where b.value = a.value and a.key > '9'
   ) 
INSERT OVERWRITE TABLE src_5 
  select *  
  where b.key not in  ( select key from src s1 where s1.key > '2') 
  order by key 
;

select * from src_4
;
select * from src_5
;