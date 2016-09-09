set hive.auto.convert.join = true;

explain
select sum(hash(a.k1,a.v1,a.k2,a.v2,a.k3,a.v3))
from (
SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3, src3.value as v3 
FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY k1,v1,k2,v2,k3,v3
)a;

select sum(hash(a.k1,a.v1,a.k2,a.v2,a.k3,a.v3))
from (
SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3, src3.value as v3 
FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY k1,v1,k2,v2,k3,v3
)a;

explain
select sum(hash(a.k1,a.v1,a.k2,a.v2,a.k3,a.v3))
from (
SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3, src3.value as v3  
FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key < 15) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY k1,v1,k2,v2,k3,v3
)a;

select sum(hash(a.k1,a.v1,a.k2,a.v2,a.k3,a.v3))
from (
SELECT src1.key as k1, src1.value as v1, src2.key as k2, src2.value as v2 , src3.key as k3, src3.value as v3  
FROM src src1 JOIN src src2 ON (src1.key = src2.key AND src1.key < 10 AND src2.key < 15) RIGHT OUTER JOIN src src3 ON (src1.key = src3.key AND src3.key < 20)
SORT BY k1,v1,k2,v2,k3,v3
)a;
