

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

select *
from part x 
where x.p_name in (select y.p_name from part y where exists (select z.p_name from part z where y.p_name = z.p_name))
;