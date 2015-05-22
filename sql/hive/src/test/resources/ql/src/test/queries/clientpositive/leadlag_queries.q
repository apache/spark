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

-- 1. testLeadUDAF
select p_mfgr, p_retailprice,
lead(p_retailprice) over (partition by p_mfgr order by p_name) as l1,
lead(p_retailprice,1) over (partition by p_mfgr order by p_name) as l2,
lead(p_retailprice,1,10) over (partition by p_mfgr order by p_name) as l3,
lead(p_retailprice,1, p_retailprice) over (partition by p_mfgr order by p_name) as l4,
p_retailprice - lead(p_retailprice,1,p_retailprice) over (partition by p_mfgr order by p_name)
from part;

-- 2.testLeadUDAFPartSz1
select p_mfgr, p_name, p_retailprice,
lead(p_retailprice,1) over (partition by p_mfgr, p_name ),
p_retailprice - lead(p_retailprice,1,p_retailprice) over (partition by p_mfgr, p_name)
from part;

-- 3.testLagUDAF
select p_mfgr, p_retailprice,
lag(p_retailprice,1) over (partition by p_mfgr order by p_name) as l1,
lag(p_retailprice) over (partition by p_mfgr order by p_name) as l2,
lag(p_retailprice,1, p_retailprice) over (partition by p_mfgr order by p_name) as l3,
lag(p_retailprice,1,10) over (partition by p_mfgr order by p_name) as l4,
p_retailprice - lag(p_retailprice,1,p_retailprice) over (partition by p_mfgr order by p_name)
from part;

-- 4.testLagUDAFPartSz1
select p_mfgr, p_name, p_retailprice,
lag(p_retailprice,1) over (partition by p_mfgr, p_name ),
p_retailprice - lag(p_retailprice,1,p_retailprice) over (partition by p_mfgr, p_name)
from part;

-- 5.testLeadLagUDAF
select p_mfgr, p_retailprice,
lead(p_retailprice,1) over (partition by p_mfgr order by p_name) as l1,
lead(p_retailprice,1, p_retailprice) over (partition by p_mfgr order by p_name) as l2,
p_retailprice - lead(p_retailprice,1,p_retailprice) over (partition by p_mfgr order by p_name),
lag(p_retailprice,1) over (partition by p_mfgr order by p_name) as l3,
lag(p_retailprice,1, p_retailprice) over (partition by p_mfgr order by p_name)  as l4
from part;
