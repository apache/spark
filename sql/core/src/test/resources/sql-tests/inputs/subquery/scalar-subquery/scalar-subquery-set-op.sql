-- Set operations in correlation path

--ONLY_IF spark
CREATE OR REPLACE TEMP VIEW t0(t0a, t0b) AS VALUES (1, 1), (2, 0);
CREATE OR REPLACE TEMP VIEW t1(t1a, t1b, t1c) AS VALUES (1, 1, 3);
CREATE OR REPLACE TEMP VIEW t2(t2a, t2b, t2c) AS VALUES (1, 1, 5), (2, 2, 7);


-- UNION ALL

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2a = t0a)
)
FROM t0;

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  UNION ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
)
FROM t0;

SELECT t0a, (SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Tests for column aliasing
SELECT t0a, (SELECT sum(t1a + 3 * t1b + 5 * t1c) FROM
  (SELECT t1c as t1a, t1a as t1b, t0a as t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t0a as t2b, t2c as t1a, t0b as t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Test handling of COUNT bug
SELECT t0a, (SELECT count(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION ALL
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Correlated references in project
SELECT t0a, (SELECT sum(d) FROM
  (SELECT t1a - t0a as d
  FROM   t1
  UNION ALL
  SELECT t2a - t0a as d
  FROM   t2)
)
FROM t0;

-- Correlated references in aggregate - unsupported
SELECT t0a, (SELECT sum(d) FROM
  (SELECT sum(t0a) as d
  FROM   t1
  UNION ALL
  SELECT sum(t2a) + t0a as d
  FROM   t2)
)
FROM t0;



-- UNION DISTINCT

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2a = t0a)
)
FROM t0;

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  UNION DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
)
FROM t0;

SELECT t0a, (SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Tests for column aliasing
SELECT t0a, (SELECT sum(t1a + 3 * t1b + 5 * t1c) FROM
  (SELECT t1c as t1a, t1a as t1b, t0a as t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t0a as t2b, t2c as t1a, t0b as t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Test handling of COUNT bug
SELECT t0a, (SELECT count(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  UNION DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Correlated references in project
SELECT t0a, (SELECT sum(d) FROM
  (SELECT t1a - t0a as d
  FROM   t1
  UNION DISTINCT
  SELECT t2a - t0a as d
  FROM   t2)
)
FROM t0;

-- Correlated references in aggregate - unsupported
SELECT t0a, (SELECT sum(d) FROM
  (SELECT sum(t0a) as d
  FROM   t1
  UNION DISTINCT
  SELECT sum(t2a) + t0a as d
  FROM   t2)
)
FROM t0;


-- INTERSECT ALL

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2a = t0a)
)
FROM t0;

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  INTERSECT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
)
FROM t0;

SELECT t0a, (SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT ALL
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Tests for column aliasing
SELECT t0a, (SELECT sum(t1a + 3 * t1b + 5 * t1c) FROM
  (SELECT t1c as t1a, t1a as t1b, t0a as t1c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT ALL
  SELECT t0a as t2b, t2c as t1a, t0b as t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Test handling of COUNT bug
SELECT t0a, (SELECT count(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT ALL
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Correlated references in project
SELECT t0a, (SELECT sum(d) FROM
  (SELECT t1a - t0a as d
  FROM   t1
  INTERSECT ALL
  SELECT t2a - t0a as d
  FROM   t2)
)
FROM t0;

-- Correlated references in aggregate - unsupported
SELECT t0a, (SELECT sum(d) FROM
  (SELECT sum(t0a) as d
  FROM   t1
  INTERSECT ALL
  SELECT sum(t2a) + t0a as d
  FROM   t2)
)
FROM t0;



-- INTERSECT DISTINCT

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2a = t0a)
)
FROM t0;

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  INTERSECT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
)
FROM t0;

SELECT t0a, (SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Tests for column aliasing
SELECT t0a, (SELECT sum(t1a + 3 * t1b + 5 * t1c) FROM
  (SELECT t1c as t1a, t1a as t1b, t0a as t1c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT DISTINCT
  SELECT t0a as t2b, t2c as t1a, t0b as t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Test handling of COUNT bug
SELECT t0a, (SELECT count(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  INTERSECT DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Correlated references in project
SELECT t0a, (SELECT sum(d) FROM
  (SELECT t1a - t0a as d
  FROM   t1
  INTERSECT DISTINCT
  SELECT t2a - t0a as d
  FROM   t2)
)
FROM t0;

-- Correlated references in aggregate - unsupported
SELECT t0a, (SELECT sum(d) FROM
  (SELECT sum(t0a) as d
  FROM   t1
  INTERSECT DISTINCT
  SELECT sum(t2a) + t0a as d
  FROM   t2)
)
FROM t0;



-- EXCEPT ALL

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2a = t0a)
)
FROM t0;

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  EXCEPT ALL
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
)
FROM t0;

SELECT t0a, (SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT ALL
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Tests for column aliasing
SELECT t0a, (SELECT sum(t1a + 3 * t1b + 5 * t1c) FROM
  (SELECT t1c as t1a, t1a as t1b, t0a as t1c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT ALL
  SELECT t0a as t2b, t2c as t1a, t0b as t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Test handling of COUNT bug
SELECT t0a, (SELECT count(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT ALL
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Correlated references in project
SELECT t0a, (SELECT sum(d) FROM
  (SELECT t1a - t0a as d
  FROM   t1
  EXCEPT ALL
  SELECT t2a - t0a as d
  FROM   t2)
)
FROM t0;

-- Correlated references in aggregate - unsupported
SELECT t0a, (SELECT sum(d) FROM
  (SELECT sum(t0a) as d
  FROM   t1
  EXCEPT ALL
  SELECT sum(t2a) + t0a as d
  FROM   t2)
)
FROM t0;



-- EXCEPT DISTINCT

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

SELECT * FROM t0 WHERE t0a <
(SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b = t0b)
);

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2a = t0a)
)
FROM t0;

SELECT t0a, (SELECT sum(c) FROM
  (SELECT t1c as c
  FROM   t1
  WHERE  t1a > t0a
  EXCEPT DISTINCT
  SELECT t2c as c
  FROM   t2
  WHERE  t2b <= t0b)
)
FROM t0;

SELECT t0a, (SELECT sum(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Tests for column aliasing
SELECT t0a, (SELECT sum(t1a + 3 * t1b + 5 * t1c) FROM
  (SELECT t1c as t1a, t1a as t1b, t0a as t1c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT DISTINCT
  SELECT t0a as t2b, t2c as t1a, t0b as t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Test handling of COUNT bug
SELECT t0a, (SELECT count(t1c) FROM
  (SELECT t1c
  FROM   t1
  WHERE  t1a = t0a
  EXCEPT DISTINCT
  SELECT t2c
  FROM   t2
  WHERE  t2b = t0b)
)
FROM t0;

-- Correlated references in project
SELECT t0a, (SELECT sum(d) FROM
  (SELECT t1a - t0a as d
  FROM   t1
  EXCEPT DISTINCT
  SELECT t2a - t0a as d
  FROM   t2)
)
FROM t0;

-- Correlated references in aggregate - unsupported
SELECT t0a, (SELECT sum(d) FROM
  (SELECT sum(t0a) as d
  FROM   t1
  EXCEPT DISTINCT
  SELECT sum(t2a) + t0a as d
  FROM   t2)
)
FROM t0;

-- Correlated references in join predicates
SELECT t0a, (SELECT sum(t1b) FROM
  (SELECT t1b
  FROM   t1 join t2 ON (t1a = t0a and t1b = t2b)
  UNION ALL
  SELECT t2b
  FROM   t1 join t2 ON (t2a = t0a and t1a = t2a))
)
FROM t0;


SELECT t0a, (SELECT sum(t1b) FROM
  (SELECT t1b
  FROM   t1 left join t2 ON (t1a = t0a and t1b = t2b)
  UNION ALL
  SELECT t2b
  FROM   t1 join t2 ON (t2a = t0a + 1 and t1a = t2a))
)
FROM t0;
