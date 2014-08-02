
create table test_union_lateral_view(key int, arr_ele int, value string);

EXPLAIN 
INSERT OVERWRITE TABLE test_union_lateral_view
SELECT b.key, d.arr_ele, d.value
FROM (
 SELECT c.arr_ele as arr_ele, a.key as key, a.value as value
 FROM (
   SELECT key, value, array(1,2,3) as arr
   FROM src

   UNION ALL
   
   SELECT key, value, array(1,2,3) as arr
   FROM srcpart
   WHERE ds = '2008-04-08' and hr='12'
 ) a LATERAL VIEW EXPLODE(arr) c AS arr_ele
) d
LEFT OUTER JOIN src b
ON d.key = b.key
;

INSERT OVERWRITE TABLE test_union_lateral_view
SELECT b.key, d.arr_ele, d.value
FROM (
 SELECT c.arr_ele as arr_ele, a.key as key, a.value as value
 FROM (
   SELECT key, value, array(1,2,3) as arr
   FROM src

   UNION ALL
   
   SELECT key, value, array(1,2,3) as arr
   FROM srcpart
   WHERE ds = '2008-04-08' and hr='12'
 ) a LATERAL VIEW EXPLODE(arr) c AS arr_ele
) d
LEFT OUTER JOIN src b
ON d.key = b.key
;

select key, arr_ele, value from test_union_lateral_view order by key, arr_ele limit 20;
