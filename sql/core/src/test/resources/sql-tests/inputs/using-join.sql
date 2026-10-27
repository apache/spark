create temporary view nt1 as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3)
  as nt1(k, v1);

create temporary view nt2 as select * from values
  ("one", 1),
  ("two", 22),
  ("one", 5),
  ("four", 4)
  as nt2(k, v2);

SELECT * FROM nt1 left outer join nt2 using (k);

SELECT k FROM nt1 left outer join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 left outer join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 left outer join nt2 using (k);

SELECT k FROM (SELECT nt2.k FROM nt1 left outer join nt2 using (k));

SELECT nt2.k AS key FROM nt1 left outer join nt2 using (k) ORDER BY key;

SELECT nt1.k, nt2.k FROM nt1 left outer join nt2 using (k) ORDER BY nt2.k;

SELECT k, nt1.k FROM nt1 left outer join nt2 using (k);

SELECT k, nt2.k FROM nt1 left outer join nt2 using (k);

SELECT * FROM nt1 left semi join nt2 using (k);

SELECT k FROM nt1 left semi join nt2 using (k);

SELECT nt1.* FROM nt1 left semi join nt2 using (k);

SELECT nt1.k FROM nt1 left semi join nt2 using (k);

SELECT k, nt1.k FROM nt1 left semi join nt2 using (k);

SELECT * FROM nt1 right outer join nt2 using (k);

SELECT k FROM nt1 right outer join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 right outer join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 right outer join nt2 using (k);

SELECT k FROM (SELECT nt1.k FROM nt1 right outer join nt2 using (k));

SELECT nt1.k AS key FROM nt1 right outer join nt2 using (k) ORDER BY key;

SELECT k, nt1.k FROM nt1 right outer join nt2 using (k);

SELECT k, nt2.k FROM nt1 right outer join nt2 using (k);

SELECT * FROM nt1 full outer join nt2 using (k);

SELECT k FROM nt1 full outer join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 full outer join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 full outer join nt2 using (k);

SELECT k FROM (SELECT nt2.k FROM nt1 full outer join nt2 using (k));

SELECT nt2.k AS key FROM nt1 full outer join nt2 using (k) ORDER BY key;

SELECT k, nt1.k FROM nt1 full outer join nt2 using (k);

SELECT k, nt2.k FROM nt1 full outer join nt2 using (k);

SELECT * FROM nt1 full outer join nt2 using (k);

SELECT k FROM nt1 inner join nt2 using (k);

SELECT nt1.*, nt2.* FROM nt1 inner join nt2 using (k);

SELECT nt1.k, nt2.k FROM nt1 inner join nt2 using (k);

SELECT k FROM (SELECT nt2.k FROM nt1 inner join nt2 using (k));

SELECT nt2.k AS key FROM nt1 inner join nt2 using (k) ORDER BY key;

SELECT k, nt1.k FROM nt1 inner join nt2 using (k);

SELECT k, nt2.k FROM nt1 inner join nt2 using (k);

WITH
  t1 AS (select key from values ('a') t(key)),
  t2 AS (select key from values ('a') t(key))
SELECT t1.key
FROM t1 FULL OUTER JOIN t2 USING (key)
WHERE t1.key NOT LIKE 'bb.%';

-- ResolvesNameByHiddenOutput.retainOriginalOutput shouldn't change NameScope output
SELECT *
FROM
    ( SELECT col1 FROM VALUES(false) ) LEFT JOIN ( SELECT col1 FROM VALUES(false) ) b USING(col1)
WHERE b.col1;

-- A copy of nt2 in a later join must not replace the hidden nt2.k in nt2.*
SELECT nt2.* FROM (nt1 left outer join nt2 using (k)) join nt2 x on x.v2 = 1;

-- Same, with the hidden nt2.k in the join condition
SELECT nt2.k, x.k FROM (nt1 left outer join nt2 using (k)) join nt2 x on nt2.k = x.k;

-- A top-level join without a SELECT, with the hidden nt2.k in the join condition
FROM nt1 JOIN nt2 USING (k) JOIN nt2 x ON nt2.k = x.k;

-- The same join with a correlated scalar subquery in the condition, which a Join cannot host
FROM nt1 JOIN nt2 USING (k) JOIN nt2 x
  ON nt2.k = x.k AND x.v2 = (SELECT max(y.v2) FROM nt2 y WHERE y.k = x.k);
