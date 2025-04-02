DROP TABLE IF EXISTS part;

-- data setup
CREATE TABLE part( 
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DECIMAL(6,2),
    p_comment STRING
);

LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table part;

-- 1. aggregate functions with decimal type

select p_mfgr, p_retailprice,
lead(p_retailprice) over (partition by p_mfgr ORDER BY p_name) as c1,
lag(p_retailprice) over (partition by p_mfgr ORDER BY p_name) as c2,
first_value(p_retailprice) over (partition by p_mfgr ORDER BY p_name) as c3,
last_value(p_retailprice) over (partition by p_mfgr ORDER BY p_name) as c4
from part;

-- 2. ranking functions with decimal type

select p_mfgr, p_retailprice,
row_number() over (PARTITION BY p_mfgr ORDER BY p_retailprice) as c1,
rank() over (PARTITION BY p_mfgr ORDER BY p_retailprice) as c2,
dense_rank() over (PARTITION BY p_mfgr ORDER BY p_retailprice) as c3,
percent_rank() over (PARTITION BY p_mfgr ORDER BY p_retailprice) as c4,
cume_dist() over (PARTITION BY p_mfgr ORDER BY p_retailprice) as c5,
ntile(5) over (PARTITION BY p_mfgr ORDER BY p_retailprice) as c6
from part;

-- 3. order by decimal

select p_mfgr, p_retailprice,
lag(p_retailprice) over (partition by p_mfgr ORDER BY p_retailprice desc) as c1
from part;

-- 4. partition by decimal

select p_mfgr, p_retailprice,
lag(p_retailprice) over (partition by p_retailprice) as c1
from part;

