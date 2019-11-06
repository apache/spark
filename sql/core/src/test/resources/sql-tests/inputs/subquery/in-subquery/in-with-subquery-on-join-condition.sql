-- A test suite for subquery IN join `on` condition
-- It includes correlated cases.


create temporary view s1 as select * from values
    (1), (3), (5), (7), (9)
  as s1(id);

create temporary view s2 as select * from values
    (1), (3), (4), (6), (9)
  as s2(id);

create temporary view s3 as select * from values
    (3), (4), (6), (9)
  as s3(id);


SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id IN (SELECT 9);


SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id NOT IN (SELECT 9);


-- IN with Subquery ON INNER JOIN
SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON LEFT SEMI JOIN
SELECT s1.id AS id2 FROM s1
LEFT SEMI JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON LEFT ANTI JOIN
SELECT s1.id as id2 FROM s1
LEFT ANTI JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON LEFT OUTER JOIN
SELECT s1.id, s2.id as id2 FROM s1
LEFT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON RIGHT OUTER JOIN
SELECT s1.id, s2.id as id2 FROM s1
RIGHT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- IN with Subquery ON FULL OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
FULL OUTER JOIN s2
ON s1.id = s2.id
AND s1.id IN (SELECT id FROM s3);


-- NOT IN with Subquery ON INNER JOIN
SELECT s1.id FROM s1
JOIN s2 ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON LEFT SEMI JOIN
SELECT s1.id AS id2 FROM s1
LEFT SEMI JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON LEFT ANTI JOIN
SELECT s1.id AS id2 FROM s1
LEFT ANTI JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON LEFT OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
LEFT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON RIGHT OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
RIGHT OUTER JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);


-- NOT IN with Subquery ON FULL OUTER JOIN
SELECT s1.id, s2.id AS id2 FROM s1
FULL OUTER JOIN s2
ON s1.id = s2.id
AND s1.id NOT IN (SELECT id FROM s3);
