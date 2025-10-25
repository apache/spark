create temporary view nt1 as select * from values
  ("one", 1),
  ("two", 2),
  ("three", 3)
  as nt1(k, v1);

create temporary view nt2 as select * from values
  ("one", 1),
  ("two", 22),
  ("one", 5)
  as nt2(k, v2);

create temporary view nt3 as select * from values
  ("one", 4),
  ("two", 5),
  ("one", 6)
  as nt3(k, v3);

create temporary view nt4 as select * from values
  ("one", 7),
  ("two", 8),
  ("one", 9)
  as nt4(k, v4);

SELECT * FROM nt1 natural join nt2;

SELECT * FROM nt1 natural join nt2 where k = "one";

SELECT * FROM nt1 natural left join nt2 order by v1, v2;

SELECT * FROM nt1 natural right join nt2 order by v1, v2;

SELECT count(*) FROM nt1 natural full outer join nt2;

SELECT k FROM nt1 natural join nt2;

SELECT k FROM nt1 natural join nt2 where k = "one";

SELECT nt1.* FROM nt1 natural join nt2;

SELECT nt2.* FROM nt1 natural join nt2;

SELECT sbq.* from (SELECT * FROM nt1 natural join nt2) sbq;

SELECT sbq.k from (SELECT * FROM nt1 natural join nt2) sbq;

SELECT nt1.*, nt2.* FROM nt1 natural join nt2;

SELECT *, nt2.k FROM nt1 natural join nt2;

SELECT nt1.k, nt2.k FROM nt1 natural join nt2;

SELECT k FROM (SELECT nt2.k FROM nt1 natural join nt2);

SELECT nt2.k AS key FROM nt1 natural join nt2 ORDER BY key;

SELECT nt1.k, nt2.k FROM nt1 natural join nt2 where k = "one";

SELECT * FROM (SELECT * FROM nt1 natural join nt2);

SELECT * FROM (SELECT nt1.*, nt2.* FROM nt1 natural join nt2);

SELECT * FROM (SELECT nt1.v1, nt2.k FROM nt1 natural join nt2);

SELECT nt2.k FROM (SELECT * FROM nt1 natural join nt2);

SELECT * FROM nt1 natural join nt2 natural join nt3;

SELECT nt1.*, nt2.*, nt3.* FROM nt1 natural join nt2 natural join nt3;

SELECT nt1.*, nt2.*, nt3.* FROM nt1 natural join nt2 join nt3 on nt2.k = nt3.k;

SELECT * FROM nt1 natural join nt2 join nt3 on nt1.k = nt3.k;

SELECT * FROM nt1 natural join nt2 join nt3 on nt2.k = nt3.k;

SELECT nt1.*, nt2.*, nt3.*, nt4.* FROM nt1 natural join nt2 natural join nt3 natural join nt4;

-- Join names should be computed as intersection of names of the left and right sides.
CREATE TEMPORARY VIEW nat_t2(col1 INT, col2 STRING) AS VALUES (1, 'a'), (2, 'b');
SELECT * FROM nat_t2 as t2_1 LEFT JOIN nat_t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN nat_t2 as t2_3;
SELECT * FROM nat_t2 as t2_1 RIGHT JOIN nat_t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN nat_t2 as t2_3;
SELECT * FROM nat_t2 as t2_1 CROSS JOIN nat_t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN nat_t2 as t2_3;
SELECT * FROM nat_t2 as t2_1 INNER JOIN nat_t2 as t2_2 ON t2_1.col1 = t2_2.col1
    NATURAL JOIN nat_t2 as t2_3;

-- Retain original join output under Project/Aggregate/Filter
CREATE TEMPORARY VIEW nat_t3(col1 INT) AS VALUES (1), (2), (3);
CREATE TEMPORARY VIEW nat_t4(col1 INT, col2 STRING) AS VALUES (1, 'x'), (2, 'y');
SELECT 1 FROM nat_t2 NATURAL JOIN nat_t3 JOIN nat_t4 ON nat_t4.col1 = nat_t3.col1 WHERE nat_t4.col2 = 'x';
SELECT 1 FROM nat_t2 NATURAL JOIN nat_t3 JOIN nat_t4 ON nat_t4.col1 = nat_t3.col1 GROUP BY nat_t4.col2 HAVING nat_t4.col2 = 'x';
SELECT 1 FROM nat_t2 NATURAL JOIN nat_t3 JOIN nat_t4 ON nat_t4.col1 = nat_t3.col1 ORDER BY nat_t4.col2;

DROP VIEW nat_t2;
DROP VIEW nat_t3;
DROP VIEW nat_t4;
