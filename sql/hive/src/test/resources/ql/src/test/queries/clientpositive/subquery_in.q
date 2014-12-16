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

DROP TABLE lineitem;
CREATE TABLE lineitem (L_ORDERKEY      INT,
                                L_PARTKEY       INT,
                                L_SUPPKEY       INT,
                                L_LINENUMBER    INT,
                                L_QUANTITY      DOUBLE,
                                L_EXTENDEDPRICE DOUBLE,
                                L_DISCOUNT      DOUBLE,
                                L_TAX           DOUBLE,
                                L_RETURNFLAG    STRING,
                                L_LINESTATUS    STRING,
                                l_shipdate      STRING,
                                L_COMMITDATE    STRING,
                                L_RECEIPTDATE   STRING,
                                L_SHIPINSTRUCT  STRING,
                                L_SHIPMODE      STRING,
                                L_COMMENT       STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA LOCAL INPATH '../../data/files/lineitem.txt' OVERWRITE INTO TABLE lineitem;

-- non agg, non corr
explain
 select * 
from src 
where src.key in (select key from src s1 where s1.key > '9')
;

select * 
from src 
where src.key in (select key from src s1 where s1.key > '9')
order by key
;

-- non agg, corr
explain 
select * 
from src b 
where b.key in
        (select a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;

select * 
from src b 
where b.key in
        (select a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
order by b.key
;

-- agg, non corr
explain
select p_name, p_size 
from 
part where part.p_size in 
	(select avg(p_size) 
	 from (select p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2
	)
;
select p_name, p_size 
from 
part where part.p_size in 
	(select avg(p_size) 
	 from (select p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2
	)
order by p_name
;

-- agg, corr
explain
select p_mfgr, p_name, p_size 
from part b where b.p_size in 
	(select min(p_size) 
	 from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2 and b.p_mfgr = a.p_mfgr
	)
;

select p_mfgr, p_name, p_size 
from part b where b.p_size in 
	(select min(p_size) 
	 from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a 
	 where r <= 2 and b.p_mfgr = a.p_mfgr
	)
order by p_mfgr, p_name, p_size 
;

-- distinct, corr
explain 
select * 
from src b 
where b.key in
        (select distinct a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
;

select * 
from src b 
where b.key in
        (select distinct a.key 
         from src a 
         where b.value = a.value and a.key > '9'
        )
order by b.key
;

-- non agg, non corr, windowing
select p_mfgr, p_name, p_size 
from part 
where part.p_size in 
  (select first_value(p_size) over(partition by p_mfgr order by p_size) from part)
order by p_mfgr, p_name, p_size 
;

-- non agg, non corr, with join in Parent Query
explain
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR')
;

select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR')
order by p.p_partkey, li.l_suppkey 
;

-- non agg, corr, with join in Parent Query
select p.p_partkey, li.l_suppkey 
from (select distinct l_partkey as p_partkey from lineitem) p join lineitem li on p.p_partkey = li.l_partkey 
where li.l_linenumber = 1 and
 li.l_orderkey in (select l_orderkey from lineitem where l_shipmode = 'AIR' and l_linenumber = li.l_linenumber)
order by p.p_partkey, li.l_suppkey 
;
