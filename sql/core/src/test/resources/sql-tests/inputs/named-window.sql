-- Tests for named window references (WINDOW clause).

-- Test data: simplified TPC-H nation and region tables.
CREATE OR REPLACE TEMPORARY VIEW nation AS SELECT * FROM VALUES
(0,  'ALGERIA',      0),
(1,  'ARGENTINA',    1),
(2,  'BRAZIL',       1),
(3,  'CANADA',       1),
(4,  'EGYPT',        4),
(5,  'ETHIOPIA',     0),
(6,  'FRANCE',       3),
(7,  'GERMANY',      3),
(8,  'INDIA',        2),
(9,  'INDONESIA',    2),
(10, 'IRAN',         4),
(11, 'IRAQ',         4),
(12, 'JAPAN',        2),
(13, 'JORDAN',       4),
(14, 'KENYA',        0),
(15, 'MOROCCO',      0),
(16, 'MOZAMBIQUE',   0),
(17, 'PERU',         1),
(18, 'CHINA',        2),
(19, 'ROMANIA',      3),
(20, 'SAUDI ARABIA', 4),
(21, 'VIETNAM',      2),
(22, 'RUSSIA',       3),
(23, 'UNITED KINGDOM', 3),
(24, 'UNITED STATES', 1)
AS nation(n_nationkey, n_name, n_regionkey);

CREATE OR REPLACE TEMPORARY VIEW region AS SELECT * FROM VALUES
(0, 'AFRICA'),
(1, 'AMERICA'),
(2, 'ASIA'),
(3, 'EUROPE'),
(4, 'MIDDLE EAST')
AS region(r_regionkey, r_name);

-- Basic named window reference: OVER w with WINDOW clause.
SELECT n_nationkey, n_name, sum(n_nationkey) OVER w AS ws
FROM nation
WINDOW w AS (ORDER BY n_nationkey)
ORDER BY n_nationkey;

-- Named window reference with PARTITION BY.
SELECT n_regionkey, n_nationkey, sum(n_nationkey) OVER w AS ws
FROM nation
WINDOW w AS (PARTITION BY n_regionkey ORDER BY n_nationkey)
ORDER BY n_regionkey, n_nationkey;

-- Multiple window functions referencing the same named window.
SELECT n_nationkey, n_name,
       sum(n_nationkey) OVER w AS total,
       rank() OVER w AS rnk
FROM nation
WINDOW w AS (PARTITION BY n_regionkey ORDER BY n_nationkey)
ORDER BY n_regionkey, n_nationkey;

-- Multiple named windows with different specs, each used by a different function.
SELECT n_regionkey, n_nationkey,
       sum(n_nationkey) OVER w1 AS running_sum,
       rank() OVER w2 AS rgn_rank
FROM nation
WINDOW
  w1 AS (ORDER BY n_nationkey),
  w2 AS (PARTITION BY n_regionkey ORDER BY n_nationkey)
ORDER BY n_nationkey;

-- Forward reference: w1 references w2 which is defined after it.
SELECT n_nationkey, sum(n_nationkey) OVER w1 AS s
FROM nation
WINDOW w1 AS w2, w2 AS (ORDER BY n_nationkey)
ORDER BY n_nationkey;

-- Backward reference: w2 references w1 which is defined before it.
SELECT n_nationkey, sum(n_nationkey) OVER w2 AS s
FROM nation
WINDOW w1 AS (ORDER BY n_nationkey), w2 AS w1
ORDER BY n_nationkey;

-- Multiple windows aliasing the same inline spec: w2 and w3 both reference w1.
SELECT n_nationkey,
       sum(n_nationkey) OVER w2 AS s2,
       rank() OVER w3 AS r3
FROM nation
WINDOW w1 AS (ORDER BY n_nationkey), w2 AS w1, w3 AS w1
ORDER BY n_nationkey;

-- Circular reference: w1 AS w2, w2 AS w1 -- neither resolves to an inline spec.
SELECT sum(n_nationkey) OVER w1 FROM nation WINDOW w1 AS w2, w2 AS w1;

-- Multi-level indirection: w3 references w2 which references w1 (error: ref to ref).
SELECT sum(n_nationkey) OVER w3 FROM nation WINDOW w1 AS (ORDER BY n_nationkey), w2 AS w1, w3 AS w2;

-- OVER w without a WINDOW clause should produce an error.
SELECT sum(n_nationkey) OVER w FROM nation;

-- A window defined in the WINDOW clause but not referenced should be silently accepted.
SELECT n_nationkey, n_name, rank() OVER w1 AS rnk
FROM nation
WINDOW
  w1 AS (PARTITION BY n_regionkey ORDER BY n_nationkey),
  w2 AS (ORDER BY n_name)
ORDER BY n_regionkey, n_nationkey;

-- Window definition inside a CTE body is local to the CTE and works correctly.
WITH cte AS (
  SELECT n_nationkey, sum(n_nationkey) OVER w AS s
  FROM nation
  WINDOW w AS (ORDER BY n_nationkey)
)
SELECT * FROM cte
ORDER BY n_nationkey;

-- Named window inside a FROM subquery works independently of the outer query.
SELECT * FROM (
  SELECT n_nationkey, sum(n_nationkey) OVER w AS s
  FROM nation
  WINDOW w AS (ORDER BY n_nationkey)
) sub
ORDER BY n_nationkey;

-- Nested subqueries: only the innermost FROM subquery defines WINDOW w.
SELECT * FROM (
  SELECT * FROM (
    SELECT n_nationkey, sum(n_nationkey) OVER w AS s
    FROM nation
    WINDOW w AS (ORDER BY n_nationkey)
  ) inner_sub
) outer_sub
ORDER BY n_nationkey;

-- Same window name 'w' independently defined in outer query and FROM subquery.
SELECT n_nationkey, n_regionkey, inner_sum, sum(n_nationkey) OVER w AS outer_sum
FROM (
  SELECT n_nationkey, n_regionkey, sum(n_nationkey) OVER w AS inner_sum
  FROM nation
  WINDOW w AS (ORDER BY n_nationkey)
) sub
WINDOW w AS (PARTITION BY n_regionkey ORDER BY n_nationkey)
ORDER BY n_regionkey, n_nationkey;

-- Outer WINDOW clause is not visible inside a CTE body (error).
WITH cte AS (
  SELECT sum(n_nationkey) OVER w AS s FROM nation
)
SELECT * FROM cte
WINDOW w AS (ORDER BY n_nationkey);

-- Outer WINDOW clause is not visible inside a FROM subquery (error).
SELECT *
FROM (SELECT sum(n_nationkey) OVER w AS s FROM nation) sub
WINDOW w AS (ORDER BY n_nationkey);

-- Named window definitions are not visible inside scalar subqueries (error).
SELECT (SELECT sum(n_regionkey) OVER w FROM nation) FROM (SELECT 1) WINDOW w AS (ORDER BY n_nationkey);

-- Outer WINDOW w is not visible inside a correlated scalar subquery (error).
SELECT n_nationkey,
  (SELECT sum(r_regionkey) OVER w FROM region WHERE r_regionkey = n_regionkey) AS s
FROM nation
WINDOW w AS (ORDER BY n_nationkey)
ORDER BY n_nationkey;

-- Named window used in a query with a JOIN.
SELECT n.n_regionkey, n.n_nationkey, n.n_name,
       sum(n.n_nationkey) OVER w AS regional_sum
FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey
WINDOW w AS (PARTITION BY n.n_regionkey ORDER BY n.n_nationkey)
ORDER BY n.n_regionkey, n.n_nationkey;

-- Multiple window functions over a named window using columns from both sides of a JOIN.
SELECT n_nationkey, n_name, r_name,
  count(*) OVER w AS cnt,
  rank() OVER w AS rnk
FROM nation JOIN region ON n_regionkey = r_regionkey
WINDOW w AS (PARTITION BY r_name ORDER BY n_nationkey)
ORDER BY n_nationkey;

-- Both UNION branches define WINDOW w with different specs; each resolves independently.
SELECT n_nationkey, sum(n_nationkey) OVER w AS s
FROM nation
WINDOW w AS (ORDER BY n_nationkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
UNION ALL
SELECT r_regionkey, sum(r_regionkey) OVER w AS s
FROM region
WINDOW w AS (ORDER BY r_regionkey RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY 1, 2;

-- One UNION branch uses a named WINDOW w, the other uses an equivalent inline spec.
SELECT n_nationkey, rank() OVER w AS rk
FROM nation
WINDOW w AS (ORDER BY n_nationkey)
UNION ALL
SELECT r_regionkey, rank() OVER (ORDER BY r_regionkey) AS rk
FROM region
ORDER BY 1, 2;

-- Only one UNION branch defines WINDOW w; the other references w without defining it (error).
SELECT n_nationkey, rank() OVER w AS rk
FROM nation
WINDOW w AS (ORDER BY n_nationkey)
UNION ALL
SELECT r_regionkey, rank() OVER w AS rk
FROM region
ORDER BY 1;
