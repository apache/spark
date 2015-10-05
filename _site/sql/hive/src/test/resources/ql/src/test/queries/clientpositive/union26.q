EXPLAIN 
SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION ALL

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value
;

SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION ALL

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value order by key, value
;


SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION ALL

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value
;

SELECT 
count(1) as counts,
key,
value
FROM
(

SELECT
a.key, a.value
FROM srcpart a JOIN srcpart b 
ON a.ds='2008-04-08' and a.hr='11' and b.ds='2008-04-08' and b.hr='12'
AND a.key = b.key 

UNION ALL

select key, value 
FROM srcpart LATERAL VIEW explode(array(1,2,3)) myTable AS myCol
WHERE ds='2008-04-08' and hr='11'
) a
group by key, value order by key, value
;
