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

-- testJoinWithAmbigousAlias
select abc.* 
from noop(on part 
partition by p_mfgr 
order by p_name 
) abc join part on abc.p_partkey = p1.p_partkey;
