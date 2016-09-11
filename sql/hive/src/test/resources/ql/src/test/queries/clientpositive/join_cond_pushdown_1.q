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

LOAD DATA LOCAL INPATH '../../data/files/part_tiny.txt' overwrite into table part;



explain select *
from part p1 join part p2 join part p3 on p1.p_name = p2.p_name and p2.p_name = p3.p_name;

explain select *
from part p1 join part p2 join part p3 on p2.p_name = p1.p_name and p3.p_name = p2.p_name;

explain select *
from part p1 join part p2 join part p3 on p2.p_partkey + p1.p_partkey = p1.p_partkey and p3.p_name = p2.p_name;

explain select *
from part p1 join part p2 join part p3 on p2.p_partkey = 1 and p3.p_name = p2.p_name;
