DROP TABLE part;

-- data setup
CREATE TABLE part( 
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
);

LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table part;

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

select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) = round(sum(lag(p_retailprice,1,0.0)) over w1 + last_value(p_retailprice) over w1 , 2), 
max(p_retailprice) over w1 - min(p_retailprice) over w1 = last_value(p_retailprice) over w1 - first_value(p_retailprice) over w1
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
;
select p_mfgr, p_retailprice, p_size,
rank() over (distribute by p_mfgr sort by p_retailprice) as r,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) as s2,
sum(p_retailprice) over (distribute by p_mfgr sort by p_retailprice rows between unbounded preceding and current row) -5 as s1
from part
;

select s, si, f, si - lead(f, 3) over (partition by t order by bo,s,si,f desc) from over10k limit 100;
select s, i, i - lead(i, 3, 0) over (partition by si order by i,s) from over10k limit 100;
select s, si, d, si - lag(d, 3) over (partition by b order by si,s,d) from over10k limit 100;
select s, lag(s, 3, 'fred') over (partition by f order by b) from over10k limit 100;

select p_mfgr, avg(p_retailprice) over(partition by p_mfgr, p_type order by p_mfgr) from part;

select p_mfgr, avg(p_retailprice) over(partition by p_mfgr order by p_type,p_mfgr rows between unbounded preceding and current row) from part;

-- multi table insert test
create table t1 (a1 int, b1 string); 
create table t2 (a1 int, b1 string);
from (select sum(i) over (partition by ts order by i), s from over10k) tt insert overwrite table t1 select * insert overwrite table t2 select * ;
select * from t1 limit 3;
select * from t2 limit 3;

select p_mfgr, p_retailprice, p_size,
round(sum(p_retailprice) over w1 , 2) + 50.0 = round(sum(lag(p_retailprice,1,50.0)) over w1 + (last_value(p_retailprice) over w1),2)
from part
window w1 as (distribute by p_mfgr sort by p_retailprice)
limit 11;
