-- average with interval type

-- null
select avg(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v) where v is null;

-- empty set
select avg(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v) where 1=0;

--
select avg(cast(v as interval)) from VALUES ('1 seconds'), ('2 seconds'), (null) t(v);
select avg(cast(v as interval)) from VALUES ('-1 seconds'), ('2 seconds'), (null) t(v);
select avg(cast(v as interval)) from VALUES ('-1 seconds'), ('-2 seconds'), (null) t(v);
select avg(cast(v as interval)) from VALUES ('-1 weeks'), ('2 seconds'), (null) t(v);

--group by
select
 i,
 avg(cast(v as interval))
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
group by i;

--having
select
 avg(cast(v as interval)) as sv
from VALUES (1, '-1 weeks'), (2, '2 seconds'), (3, null), (1, '5 days') t(i, v)
having sv is not null;

-- window
SELECT
 i,
 avg(cast(v as interval)) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM VALUES(1,'1 seconds'),(1,'2 seconds'),(2,NULL),(2,NULL) t(i,v);
