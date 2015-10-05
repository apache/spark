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

select s, rank() over (partition by f order by t) from over10k limit 100;

select s, dense_rank() over (partition by ts order by i,s desc) from over10k limit 100;

select s, cume_dist() over (partition by bo order by b,s) from over10k limit 100;

select s, percent_rank() over (partition by dec order by f) from over10k limit 100;

-- If following tests fail, look for the comments in class PTFPPD::process()

select ts, dec, rnk
from
  (select ts, dec,
          rank() over (partition by ts order by dec)  as rnk
          from
            (select other.ts, other.dec
             from over10k other
             join over10k on (other.b = over10k.b)
            ) joined
  ) ranked
where rnk =  1 limit 10;

select ts, dec, rnk
from
  (select ts, dec,
          rank() over (partition by ts)  as rnk
          from
            (select other.ts, other.dec
             from over10k other
             join over10k on (other.b = over10k.b)
            ) joined
  ) ranked
where dec = 89.5 limit 10;

select ts, dec, rnk
from
  (select ts, dec,
          rank() over (partition by ts order by dec)  as rnk
          from
            (select other.ts, other.dec
             from over10k other
             join over10k on (other.b = over10k.b)
             where other.t < 10
            ) joined
  ) ranked
where rnk = 1 limit 10;

