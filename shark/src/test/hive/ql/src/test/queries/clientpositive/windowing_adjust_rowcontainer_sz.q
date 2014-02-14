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

LOAD DATA LOCAL INPATH '../data/files/part_tiny.txt' overwrite into table part;

set hive.join.cache.size=1;

select p_mfgr, p_name, p_size,
rank() over(distribute by p_mfgr sort by p_name) as r,
dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
sum(p_retailprice) over (distribute by p_mfgr sort by p_name rows between unbounded preceding and current row) as s1
from part
;

set hive.join.cache.size=25000;