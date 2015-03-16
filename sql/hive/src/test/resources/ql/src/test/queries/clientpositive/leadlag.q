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

--1. testLagWithPTFWindowing
select p_mfgr, p_name,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
p_retailprice, sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1,
p_size, p_size - lag(p_size,1,p_size) over (partition by p_mfgr order by p_name) as deltaSz
from noop(on part
partition by p_mfgr
order by p_name 
);

-- 2. testLagWithWindowingNoPTF
select p_mfgr, p_name,
rank() over (partition by p_mfgr order by p_name) as r,
dense_rank() over (partition by p_mfgr order by p_name) as dr,
p_retailprice, sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1,
p_size, p_size - lag(p_size,1,p_size) over (partition by p_mfgr order by p_name) as deltaSz
from part
;   

-- 3. testJoinWithLag
select p1.p_mfgr, p1.p_name,
p1.p_size, p1.p_size - lag(p1.p_size,1,p1.p_size) over( distribute by p1.p_mfgr sort by p1.p_name) as deltaSz
from part p1 join part p2 on p1.p_partkey = p2.p_partkey
 ;

-- 4. testLagInSum
select  p_mfgr,p_name, p_size,   
sum(p_size - lag(p_size,1)) over(distribute by p_mfgr  sort by p_name ) as deltaSum 
from part 
window w1 as (rows between 2 preceding and 2 following) ;

-- 5. testLagInSumOverWindow
select  p_mfgr,p_name, p_size,   
sum(p_size - lag(p_size,1)) over w1 as deltaSum 
from part 
window w1 as (distribute by p_mfgr sort by p_name rows between 2 preceding and 2 following) ;

-- 6. testRankInLead
select p_mfgr, p_name, p_size, r1,
lead(r1,1,r1) over (distribute by p_mfgr sort by p_name) as deltaRank
from (
select p_mfgr, p_name, p_size, 
rank() over(distribute by p_mfgr  sort by p_name) as r1 
from part 
) a;

-- 7. testLeadWithPTF
select p_mfgr, p_name, 
rank() over(distribute by p_mfgr sort by p_name) as r, 
dense_rank() over(distribute by p_mfgr sort by p_name) as dr, 
p_size, p_size - lead(p_size,1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz 
from noop(on part 
partition by p_mfgr 
order by p_name  
) 
;

-- 8. testOverNoPartitionMultipleAggregate
select p_name, p_retailprice,
lead(p_retailprice) over() as l1 ,
lag(p_retailprice) over() as l2
from part
where p_retailprice = 1173.15;

