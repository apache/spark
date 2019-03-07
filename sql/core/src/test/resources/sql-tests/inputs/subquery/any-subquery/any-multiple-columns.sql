-- A test suite for ANY/SOME predicate with multiple columns

create temporary view a as select * from values
  (1, 1),
  (null, 2),
  (3, 3),
  (4, null)
  AS a(a1, a2);

create temporary view b as select * from values
  (1, 1),
  (4, null),
  (3, 2)
  AS b(b1, b2);

create temporary view c as select * from values
  (1, 1),
  (null, null),
  (3, 2)
  AS c(c1, c2);

-- ANY predicate with `multi-column = subquery`
-- TC 01.01
SELECT a1,
       a2
FROM   a
WHERE  (a1, a2) = ANY (SELECT b1,
                              b2
                       FROM   b);

-- TC 01.02
SELECT a1
       a2
FROM   a
WHERE  NOT (a1, a2) = ANY (SELECT b1,
                                  b2
                           FROM   b);

-- TC 01.03
SELECT a1
       a2
FROM   a
WHERE  NOT (a1, a2) = ANY (SELECT c1,
                                  c2
                           FROM   c);

-- negative cases
-- TC 02.01
SELECT *
FROM   a
WHERE  (a1, a2) <= ANY (SELECT b1,
                               b2
                        FROM   b);

-- TC 02.02
SELECT *
FROM   a
WHERE  (a1, a2) != ANY (SELECT b1,
                               b2
                        FROM   b);

-- TC 02.03
SELECT *
FROM   a
WHERE  (a1, a2) = ANY (SELECT b1
                       FROM   b);
