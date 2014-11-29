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
           dec decimal(4,2),  
           bin binary)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/over10k' into table over10k;

select s, row_number() over (partition by d order by dec) from over10k limit 100;

select i, lead(s) over (partition by bin order by d,i desc) from over10k limit 100;

select i, lag(dec) over (partition by i order by s,i,dec) from over10k limit 100;

select s, last_value(t) over (partition by d order by f) from over10k limit 100;

select s, first_value(s) over (partition by bo order by s) from over10k limit 100;

select t, s, i, last_value(i) over (partition by t order by s) 
from over10k where (s = 'oscar allen' or s = 'oscar carson') and t = 10;
