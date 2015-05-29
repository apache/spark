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

-- testDuplicateWindowAlias
select p_mfgr, p_name, p_size, 
sum(p_size) over (w1) as s1, 
sum(p_size) over (w2) as s2
from part 
window w1 as (partition by p_mfgr order by p_mfgr rows between 2 preceding and 2 following), 
       w2 as w1, 
       w2 as (rows between unbounded preceding and current row); 
