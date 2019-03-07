-- A test suite for ANY/SOME predicate subquery

CREATE TEMPORARY VIEW a AS SELECT * FROM VALUES
  (1, 1),
  (null, 2),
  (3, 3),
  (4, 4)
  AS a(a1, a2);

CREATE TEMPORARY VIEW b AS SELECT * FROM VALUES
  (3, 1),
  (4, 2)
  AS b(b1, b2);

CREATE TEMPORARY VIEW c AS SELECT * FROM VALUES
  (3, 1),
  (4, 2),
  (null, 3)
  AS c(c1, c2);

-- comparison operators
-- TC 01.01
SELECT *
FROM   a
WHERE  a1 > ANY (SELECT b1
                 FROM   b);

-- TC 01.02
SELECT *
FROM   a
WHERE  a1 >= ANY (SELECT b1
                  FROM   b);

-- TC 01.03
SELECT *
FROM   a
WHERE  a1 < ANY (SELECT b1
                 FROM   b);

-- TC 01.04
SELECT *
FROM   a
WHERE  a1 <= ANY (SELECT b1
                  FROM   b);

-- TC 01.05
SELECT *
FROM   a
WHERE  a1 != ANY (SELECT b1
                  FROM   b);

-- TC 01.06
SELECT *
FROM   a
WHERE  a1 = ANY (SELECT b1
                 FROM   b);

-- TC 01.07
SELECT *
FROM   a
WHERE  a1 <=> ANY (SELECT b1
                   FROM   b);

-- compare with `null`
-- TC 02.01
SELECT *
FROM   a
WHERE  a1 > ANY (SELECT c1
                 FROM   c);

-- TC 02.02
SELECT *
FROM   a
WHERE  a1 != ANY (SELECT c1
                  FROM   c);

-- TC 02.03
SELECT *
FROM   a
WHERE  a1 = ANY (SELECT c1
                 FROM   c);

-- TC 02.04
SELECT *
FROM   a
WHERE  a1 <=> ANY (SELECT c1
                   FROM   c);
-- TC 02.05
SELECT *
FROM   a
WHERE  NOT a1 != ANY (SELECT c1
                      FROM   c);

-- TC 02.06
SELECT *
FROM   a
WHERE  NOT a1 = ANY (SELECT c1
                     FROM   c);
