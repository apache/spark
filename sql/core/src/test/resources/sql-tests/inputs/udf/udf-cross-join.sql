-- Cross join detection and error checking is done in JoinSuite since explain output is
-- used in the error message and the ids are not stable. Only positive cases are checked here.
-- This test file was converted from cross-join.sql.

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

-- Cross joins with and without predicates
SELECT * FROM nt1 cross join nt2;
SELECT * FROM nt1 cross join nt2 where udf(nt1.k) = udf(nt2.k);
SELECT * FROM nt1 cross join nt2 on (udf(nt1.k) = udf(nt2.k));
SELECT * FROM nt1 cross join nt2 where udf(nt1.v1) = "1" and udf(nt2.v2) = "22";

SELECT udf(a.key), udf(b.key) FROM
(SELECT udf(k) key FROM nt1 WHERE v1 < 2) a
CROSS JOIN
(SELECT udf(k) key FROM nt2 WHERE v2 = 22) b;

-- Join reordering 
create temporary view A(a, va) as select * from nt1;
create temporary view B(b, vb) as select * from nt1;
create temporary view C(c, vc) as select * from nt1;
create temporary view D(d, vd) as select * from nt1;

-- Allowed since cross join with C is explicit
select * from ((A join B on (udf(a) = udf(b))) cross join C) join D on (udf(a) = udf(d));
-- Cross joins with non-equal predicates
SELECT * FROM nt1 CROSS JOIN nt2 ON (udf(nt1.k) > udf(nt2.k));
