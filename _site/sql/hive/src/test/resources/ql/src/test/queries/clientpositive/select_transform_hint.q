EXPLAIN
SELECT /*+MAPJOIN(a)*/ 
TRANSFORM(a.key, a.value) USING 'cat' AS (tkey, tvalue)
FROM src a join src b
on a.key = b.key;


SELECT /*+MAPJOIN(a)*/ 
TRANSFORM(a.key, a.value) USING 'cat' AS (tkey, tvalue)
FROM src a join src b
on a.key = b.key;


EXPLAIN
SELECT /*+STREAMTABLE(a)*/ 
TRANSFORM(a.key, a.value) USING 'cat' AS (tkey, tvalue)
FROM src a join src b
on a.key = b.key;


SELECT /*+STREAMTABLE(a)*/ 
TRANSFORM(a.key, a.value) USING 'cat' AS (tkey, tvalue)
FROM src a join src b
on a.key = b.key;