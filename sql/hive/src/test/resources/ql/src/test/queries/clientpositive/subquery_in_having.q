
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

-- non agg, non corr
explain
 select key, count(*) 
from src 
group by key
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;


select s1.key, count(*) from src s1 where s1.key > '9' group by s1.key order by s1.key;

select key, count(*) 
from src 
group by key
having count(*) in (select count(*) from src s1 where s1.key = '90' group by s1.key )
order by key
;

-- non agg, corr
explain
 select key, value, count(*) 
from src b
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9'  and s1.value = b.value group by s1.key )
;

-- agg, non corr
explain
select p_mfgr, avg(p_size)
from part b
group by b.p_mfgr
having b.p_mfgr in 
   (select p_mfgr 
    from part
    group by p_mfgr
    having max(p_size) - min(p_size) < 20
   )
;

-- join on agg
select b.key, min(b.value)
from src b
group by b.key
having b.key in ( select a.key
                from src a
                where a.value > 'val_9' and a.value = min(b.value)
                )
order by b.key
;

-- where and having
-- Plan is:
-- Stage 1: b semijoin sq1:src (subquery in where)
-- Stage 2: group by Stage 1 o/p
-- Stage 5: group by on sq2:src (subquery in having)
-- Stage 6: Stage 2 o/p semijoin Stage 5
explain
select key, value, count(*) 
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;

set hive.auto.convert.join=true;
-- Plan is:
-- Stage  5: group by on sq2:src (subquery in having)
-- Stage 10: hashtable for sq1:src (subquery in where)
-- Stage  2: b map-side semijoin Stage 10 o/p
-- Stage  3: Stage 2 semijoin Stage 5
-- Stage  9: construct hastable for Stage 5 o/p
-- Stage  6: Stage 2 map-side semijoin Stage 9
explain
select key, value, count(*) 
from src b
where b.key in (select key from src where src.key > '8')
group by key, value
having count(*) in (select count(*) from src s1 where s1.key > '9' group by s1.key )
;

-- non agg, non corr, windowing
explain
select p_mfgr, p_name, avg(p_size) 
from part 
group by p_mfgr, p_name
having p_name in 
  (select first_value(p_name) over(partition by p_mfgr order by p_size) from part)
;
