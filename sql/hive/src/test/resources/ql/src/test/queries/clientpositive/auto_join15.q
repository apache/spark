
set hive.auto.convert.join = true;

explain
select sum(hash(a.k1,a.v1,a.k2, a.v2))
from (
SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2
FROM src src1 JOIN src src2 ON (src1.key = src2.key) 
SORT BY k1, v1, k2, v2
) a;


select sum(hash(a.k1,a.v1,a.k2, a.v2))
from (
SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2
FROM src src1 JOIN src src2 ON (src1.key = src2.key) 
SORT BY k1, v1, k2, v2
) a;

