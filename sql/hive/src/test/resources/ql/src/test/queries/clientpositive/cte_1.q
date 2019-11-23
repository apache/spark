explain
with q1 as ( select key from src where key = '5')
select *
from q1
;

with q1 as ( select key from src where key = '5')
select *
from q1
;

-- in subquery
explain
with q1 as ( select key from src where key = '5')
select * from (select key from q1) a;

with q1 as ( select key from src where key = '5')
select * from (select key from q1) a;

-- chaining
explain
with q1 as ( select key from q2 where key = '5'),
q2 as ( select key from src where key = '5')
select * from (select key from q1) a;

with q1 as ( select key from q2 where key = '5'),
q2 as ( select key from src where key = '5')
select * from (select key from q1) a;