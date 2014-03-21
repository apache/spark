DROP TABLE part_rc;

CREATE TABLE part_rc( 
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
)  STORED AS RCFILE ;

LOAD DATA LOCAL INPATH '../data/files/part.rc' overwrite into table part_rc;

-- testWindowingPTFWithPartRC
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice)  over (partition by p_mfgr order by p_name rows between unbounded preceding and current row) as s1
from noop(on part_rc 
partition by p_mfgr 
order by p_name);
