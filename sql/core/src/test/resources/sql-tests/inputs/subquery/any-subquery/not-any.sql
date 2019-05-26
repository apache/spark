-- A test suite for NOT ANY/SOME predicate subquery

CREATE TEMPORARY VIEW a AS SELECT * FROM VALUES
  (1, 1),
  (null, 2),
  (3, 3),
  (4, 4)
  AS a(a1, a2);

CREATE TEMPORARY VIEW b AS SELECT * FROM VALUES
  (3, 1)
  AS b(b1, b2);

CREATE TEMPORARY VIEW c AS SELECT * FROM VALUES
  (3, 1),
  (null, 2)
  AS c(c1, c2);

-- NOT ANY with different comparison operators
-- TC 01.01
SELECT *
FROM   a
WHERE  NOT a1 > ANY (SELECT b1
                     FROM   b);

-- TC 01.02
SELECT *
FROM   a
WHERE  NOT a1 >= ANY (SELECT b1
                      FROM   b);

-- TC 01.03
SELECT *
FROM   a
WHERE  NOT a1 < ANY (SELECT b1
                     FROM   b);

-- TC 01.04
SELECT *
FROM   a
WHERE  NOT a1 <= ANY (SELECT b1
                      FROM   b);

-- TC 01.05
SELECT *
FROM   a
WHERE  NOT a1 != ANY (SELECT b1
                      FROM   b);

-- TC 01.06
SELECT *
FROM   a
WHERE  NOT a1 = ANY (SELECT b1
                     FROM   b);

-- TC 01.07
SELECT *
FROM   a
WHERE  NOT a1 <=> ANY (SELECT b1
                       FROM   b);

-- 'null' in subquery
-- TC 02.01
SELECT *
FROM   a
WHERE  NOT a1 > ANY (SELECT c1
                     FROM   c);

-- TC 02.02
SELECT *
FROM   a
WHERE  NOT a1 = ANY (SELECT c1
                     FROM   c);
-- TC 02.03
SELECT *
FROM   a
WHERE  NOT a1 <=> ANY (SELECT c1
                       FROM   c);

-- multiple-negation
-- TC 03.01
SELECT *
FROM   a
WHERE  NOT NOT a1 = ANY (SELECT b1
                         FROM   b);

-- TC 03.02
SELECT *
FROM   a
WHERE  NOT NOT NOT a1 <=> ANY (SELECT b1
                               FROM   b);
