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

select s, min(i) over (partition by s) from over10k limit 100;

select s, avg(f) over (partition by si order by s) from over10k limit 100;

select s, avg(i) over (partition by t, b order by s) from over10k limit 100;

select max(i) over w from over10k window w as (partition by f) limit 100;

select s, avg(d) over (partition by t order by f) from over10k limit 100;
