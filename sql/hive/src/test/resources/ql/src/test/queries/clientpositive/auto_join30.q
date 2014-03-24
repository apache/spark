set hive.auto.convert.join = true;

explain
FROM 
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

FROM 
(SELECT src.* FROM src sort by key) x
JOIN 
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

FROM 
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN 
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT src.* FROM src sort by key) x
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

FROM 
(SELECT src.* FROM src sort by key) x
RIGHT OUTER JOIN 
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT src.* FROM src sort by key) x
JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT src.* FROM src sort by key) x
LEFT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

explain
FROM 
(SELECT src.* FROM src sort by key) x
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));

FROM
(SELECT src.* FROM src sort by key) x
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Y
ON (x.key = Y.key)
RIGHT OUTER JOIN
(SELECT src.* FROM src sort by value) Z
ON (x.key = Z.key)
select sum(hash(Y.key,Y.value));
