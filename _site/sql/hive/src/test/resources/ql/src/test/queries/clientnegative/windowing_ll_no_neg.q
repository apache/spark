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
    p_retailprice DOUBLE,
    p_comment STRING
);

LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table part;


select p_mfgr, p_name, p_size,
min(p_retailprice),
rank() over(distribute by p_mfgr sort by p_name)as r,
dense_rank() over(distribute by p_mfgr sort by p_name) as dr,
p_size, p_size - lag(p_size,-1,p_size) over(distribute by p_mfgr sort by p_name) as deltaSz
from part
group by p_mfgr, p_name, p_size
;
