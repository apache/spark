select * from (with q1 as ( select key from q2 where key = '5') select * from q1) a;
