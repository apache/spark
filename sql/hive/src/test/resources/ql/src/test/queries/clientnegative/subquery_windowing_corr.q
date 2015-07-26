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


-- corr and windowing 
select p_mfgr, p_name, p_size 
from part a 
where a.p_size in 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) 
   from part b 
   where a.p_brand = b.p_brand)
;