-- test cases for the explain results of comparable maps

--SET spark.sql.adaptive.enabled = false
--SET spark.sql.mapKeyDedupPolicy=LAST_WIN;

CREATE TEMPORARY VIEW t1 AS SELECT * FROM VALUES
  (map(1, 'a', 2, 'b'), map(2, 'b', 1, 'a')),
  (map(2, 'b', 1, 'a'), map(2, 'b', 1, 'A')),
  (map(3, 'c', 1, 'a'), map(4, 'd', 3, 'c')),
  (map(3, 'c', 1, 'a'), map(3, 'c')),
  (map(3, 'c'), map(4, 'd', 3, 'c')),
  (map(1, 'a', 2, null), map(2, null, 1, 'a')),
  (map(), map(1, 'a')),
  (map(1, 'a'), map())
AS t(v1, v2);

-- Checks if `SortMapKeys` inserted correctly, the explain results
-- of following complicated query cases are shown here.

-- Combination tests of Sort/Filter/Join/Aggregate/Window + binary comparisons
EXPLAIN SELECT * FROM t1 ORDER BY v1 = v2;
EXPLAIN SELECT * FROM t1 WHERE v1 = v2 AND v1 = map_concat(v2, map(1, 'a'));
EXPLAIN SELECT * FROM t1 l, t1 r WHERE l.v1 = r.v2 AND l.v1 = map_concat(r.v2, map(1, 'a'));
EXPLAIN SELECT v1 = v2, count(1) FROM t1 GROUP BY v1 = v2;
EXPLAIN SELECT v1 = v2, count(1) OVER(PARTITION BY v1 = v2) FROM t1 ORDER BY v1;

-- Combination tests of floating-point/map value normalization
CREATE TEMPORARY VIEW t9 AS SELECT * FROM VALUES
  (map("a", 0.0D, "b", -0.0D)), (map("b", 0.0D, "a", -0.0D))
AS t(v);

EXPLAIN SELECT * FROM t9 l, t9 r WHERE l.v = r.v;
EXPLAIN SELECT v, count(1) FROM t9 GROUP BY v;
EXPLAIN SELECT v, count(1) OVER(PARTITION BY v) FROM t9 ORDER BY v;
