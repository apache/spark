drop table over10k;

create table over10k(
           t tinyint,
           si smallint,
           i int,
           b bigint,
           f float,
           d double,
           bo boolean,
           s string,
	   ts timestamp, 
           dec decimal,  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../data/files/over10k' into table over10k;

select s, rank() over (partition by s order by si), sum(b) over (partition by s order by si) from over10k limit 100;

select s, 
rank() over (partition by s order by dec desc), 
sum(b) over (partition by s order by ts desc) 
from over10k
where s = 'tom allen' or s = 'bob steinbeck';

select s, sum(i) over (partition by s), sum(f) over (partition by si) from over10k where s = 'tom allen' or s = 'bob steinbeck' ;

select s, rank() over (partition by s order by bo), rank() over (partition by si order by bin desc) from over10k
where s = 'tom allen' or s = 'bob steinbeck';

select s, sum(f) over (partition by i), row_number() over (order by f) from over10k where s = 'tom allen' or s = 'bob steinbeck';

select s, rank() over w1, 
rank() over w2 
from over10k 
where s = 'tom allen' or s = 'bob steinbeck'
window 
w1 as (partition by s order by dec), 
w2 as (partition by si order by f) 
;
