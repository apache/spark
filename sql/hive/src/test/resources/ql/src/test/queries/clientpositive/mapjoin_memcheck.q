
set hive.auto.convert.join = true;

create table src0 like src;
insert into table src0 select * from src where src.key < 10;

set hive.mapjoin.check.memory.rows=1;

explain 
select src1.key as k1, src1.value as v1, src2.key, src2.value
from src0 src1 inner join src0 src2 on src1.key = src2.key order by k1, v1;

select src1.key as k1, src1.value as v1, src2.key, src2.value
from src0 src1 inner join src0 src2 on src1.key = src2.key order by k1, v1;

drop table src0;