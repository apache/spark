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

create table src11 (key1 string, value1 string);

create table part2( 
    p2_partkey INT,
    p2_name STRING,
    p2_mfgr STRING,
    p2_brand STRING,
    p2_type STRING,
    p2_size INT,
    p2_container STRING,
    p2_retailprice DOUBLE,
    p2_comment STRING
);

-- non agg, corr
explain select * from src11 where src11.key1 in (select key from src where src11.value1 = value and key > '9');

explain select * from src a where a.key in (select key from src where a.value = value and key > '9');

-- agg, corr
explain
select p_mfgr, p_name, p_size 
from part b where b.p_size in 
  (select min(p2_size) 
    from (select p2_mfgr, p2_size, rank() over(partition by p2_mfgr order by p2_size) as r from part2) a 
    where r <= 2 and b.p_mfgr = p2_mfgr
  )
;


explain
select p_mfgr, p_name, p_size 
from part b where b.p_size in 
  (select min(p_size) 
   from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
   where r <= 2 and b.p_mfgr = p_mfgr
  )
;

-- distinct, corr
explain 
select * 
from src b 
where b.key in
        (select distinct key 
         from src 
         where b.value = value and key > '9'
        )
;

-- non agg, corr, having
explain
 select key, value, count(*) 
from src b
group by key, value
having count(*) in (select count(*) from src where src.key > '9'  and src.value = b.value group by key )
;

-- non agg, corr
explain
select p_mfgr, b.p_name, p_size 
from part b 
where b.p_name not in 
  (select p_name 
  from (select p_mfgr, p_name, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
  where r <= 2 and b.p_mfgr = p_mfgr 
  )
;