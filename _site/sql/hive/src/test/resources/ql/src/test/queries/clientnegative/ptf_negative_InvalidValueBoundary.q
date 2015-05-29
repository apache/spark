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
    p_comment STRING,
    p_complex array<int>
);

-- testInvalidValueBoundary
select  p_mfgr,p_name, p_size,   
sum(p_size) over (w1) as s ,    
dense_rank() over(w1) as dr  
from part  
window w1 as (partition by p_mfgr order by p_complex range between  2 preceding and current row);
