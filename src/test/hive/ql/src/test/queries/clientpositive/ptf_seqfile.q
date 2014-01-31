DROP TABLE part_seq;

CREATE TABLE part_seq( 
    p_partkey INT,
    p_name STRING,
    p_mfgr STRING,
    p_brand STRING,
    p_type STRING,
    p_size INT,
    p_container STRING,
    p_retailprice DOUBLE,
    p_comment STRING
) STORED AS SEQUENCEFILE ;

LOAD DATA LOCAL INPATH '../data/files/part.seq' overwrite into table part_seq;

-- testWindowingPTFWithPartSeqFile
select p_mfgr, p_name, p_size, 
rank() over (partition by p_mfgr order by p_name) as r, 
dense_rank() over (partition by p_mfgr order by p_name) as dr, 
sum(p_retailprice) over (partition by p_mfgr order by p_name rows between unbounded preceding and current row)  as s1
from noop(on part_seq 
partition by p_mfgr 
order by p_name);  
