explain
with q1 as ( select key from q2 where key = '5'),
q2 as ( select key from q1 where key = '5')
select * from (select key from q1) a;