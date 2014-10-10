DROP TABLE part;

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

-- testAmbiguousWindowDefn
select p_mfgr, p_name, p_size, 
sum(p_size) over (w1) as s1, 
sum(p_size) over (w2) as s2,
sum(p_size) over (w3) as s3
from part 
distribute by p_mfgr 
sort by p_mfgr 
window w1 as (rows between 2 preceding and 2 following), 
       w2 as (rows between unbounded preceding and current row), 
       w3 as w3;

