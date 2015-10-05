create table dst_union22(k1 string, k2 string, k3 string, k4 string) partitioned by (ds string);
create table dst_union22_delta(k0 string, k1 string, k2 string, k3 string, k4 string, k5 string) partitioned by (ds string);

insert overwrite table dst_union22 partition (ds='1')
select key, value, key , value from src;

insert overwrite table dst_union22_delta partition (ds='1')
select key, key, value, key, value, value from src;

set hive.merge.mapfiles=false;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

-- Since the inputs are small, it should be automatically converted to mapjoin

explain extended
insert overwrite table dst_union22 partition (ds='2')
select * from
(
select k1 as k1, k2 as k2, k3 as k3, k4 as k4 from dst_union22_delta where ds = '1' and k0 <= 50
union all
select a.k1 as k1, a.k2 as k2, b.k3 as k3, b.k4 as k4
from dst_union22 a left outer join (select * from dst_union22_delta where ds = '1' and k0 > 50) b on
a.k1 = b.k1 and a.ds='1'
where a.k1 > 20
)
subq;

insert overwrite table dst_union22 partition (ds='2')
select * from
(
select k1 as k1, k2 as k2, k3 as k3, k4 as k4 from dst_union22_delta where ds = '1' and k0 <= 50
union all
select a.k1 as k1, a.k2 as k2, b.k3 as k3, b.k4 as k4
from dst_union22 a left outer join (select * from dst_union22_delta where ds = '1' and k0 > 50) b on
a.k1 = b.k1 and a.ds='1'
where a.k1 > 20
)
subq;

select * from dst_union22 where ds = '2' order by k1, k2, k3, k4;
