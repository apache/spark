set hive.auto.convert.join = true;

explain
FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

FROM 
(SELECT src.* FROM src) x
JOIN 
(SELECT src.* FROM src) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

