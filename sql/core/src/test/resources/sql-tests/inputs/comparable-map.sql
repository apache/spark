-- test cases for comparable maps

--CONFIG_DIM1 spark.sql.codegen.wholeStage=true
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM1 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

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

CREATE TEMPORARY VIEW t2 AS SELECT * FROM VALUES
  (array(map(1, 'a', 2, 'b')), array(map(2, 'b', 1, 'a'))),
  (array(map(2, 'b', 1, 'a')), array(map(2, 'b', 1, 'A'))),
  (array(map(3, 'c', 1, 'a')), array(map(4, 'd', 3, 'c'))),
  (array(map(1, 'a', 2, null), null), array(map(2, null, 1, 'a'), null))
AS t(v1, v2);

CREATE TEMPORARY VIEW t3 AS SELECT * FROM VALUES
  (struct(map(1, 'a', 2, 'b'), 'x'), struct(map(2, 'b', 1, 'a'), 'x')),
  (struct(map(2, 'b', 1, 'a'), 'x'), struct(map(2, 'b', 1, 'A'), 'x')),
  (struct(map(3, 'c', 1, 'a'), 'x'), struct(map(4, 'd', 3, 'c'), 'x')),
  (struct(map(1, 'a', 2, null), null), struct(map(2, null, 1, 'a'), null))
AS t(v1, v2);

CREATE TEMPORARY VIEW t4 AS SELECT * FROM VALUES
  (map(1, map(1, 'a', 2, 'b'), 2, map(3, 'c', 4, 'd')), map(2, map(4, 'd', 3, 'c'), 1, map(2, 'b', 1, 'a'))),
  (map(2, map(4, 'd', 3, 'c'), 1, map(2, 'b', 1, 'a')), map(2, map(4, 'd', 3, 'c'), 1, map(2, 'b', 1, 'A'))),
  (map(3, map(5, 'e', 6, 'f'), 1, map(1, 'a', 2, 'b')), map(4, map(7, 'g', 8, 'h'), 3, map(6, 'f', 5, 'e'))),
  (map(1, map(1, 'a', 2, null), 2, map(3, null, 4, 'd')), map(2, map(4, 'd', 3, null), 1, map(2, null, 1, 'a')))
AS t(v1, v2);

CREATE TEMPORARY VIEW t5 AS SELECT * FROM VALUES
  (map(array(1, 2), 'a', array(3, 4), 'b'), map(array(3, 4), 'b', array(1, 2), 'a')),
  (map(array(3, 4), 'b', array(1, 2), 'a'), map(array(4, 3), 'b', array(1, 2), 'a')),
  (map(array(5, 6), 'a', array(1, 2), 'a'), map(array(2, 1), 'a', array(5, 6), 'c')),
  (map(array(1, null), 'a', array(3, null), 'b'), map(array(3, null), 'b', array(1, null), 'a'))
AS t(v1, v2);

CREATE TEMPORARY VIEW t6 AS SELECT * FROM VALUES
  (map(1, array(1, 2), 2, array(3, 4)), map(2, array(3, 4), 1, array(1, 2))),
  (map(2, array(3, 4), 1, array(1, 2)), map(2, array(4, 3), 1, array(1, 2))),
  (map(3, array(5, 6), 1, array(1, 2)), map(1, array(2, 1), 3, array(5, 6))),
  (map(1, array(1, 2), 2, null), map(2, null, 1, array(2, 1)))
AS t(v1, v2);

CREATE TEMPORARY VIEW t7 AS SELECT * FROM VALUES
  (map(struct(1), 'a', struct(2), 'b'), map(struct(2), 'b', struct(1), 'a')),
  (map(struct(2), 'b', struct(1), 'a'), map(struct(1), 'b', struct(2), 'a')),
  (map(struct(3), 'c', struct(1), 'a'), map(struct(4), 'd', struct(3), 'c')),
  (map(struct(1), 'a', struct(null), 'b'), map(struct(null), 'b', struct(1), 'a'))
AS t(v1, v2);

CREATE TEMPORARY VIEW t8 AS SELECT * FROM VALUES
  (map(1, struct('a'), 2, struct('b')), map(2, struct('b'), 1, struct('a'))),
  (map(2, struct('b'), 1, struct('a')), map(2, struct('a'), 1, struct('b'))),
  (map(3, struct('c'), 1, struct('a')), map(4, struct('d'), 3, struct('c'))),
  (map(1, struct('a'), 2, struct(null)), map(2, struct(null), 1, struct('a')))
AS t(v1, v2);

-- ORDER BY cases
SELECT * FROM t1 ORDER BY v1, v2;
SELECT * FROM t2 ORDER BY v1, v2;
SELECT * FROM t3 ORDER BY v1, v2;
SELECT * FROM t4 ORDER BY v1, v2;
SELECT * FROM t5 ORDER BY v1, v2;
SELECT * FROM t6 ORDER BY v1, v2;
SELECT * FROM t7 ORDER BY v1, v2;
SELECT * FROM t8 ORDER BY v1, v2;

-- FILTER cases
SELECT * FROM t1 WHERE v1 = v2;
SELECT * FROM t2 WHERE v1 = v2;
SELECT * FROM t3 WHERE v1 = v2;
SELECT * FROM t4 WHERE v1 = v2;
SELECT * FROM t5 WHERE v1 = v2;
SELECT * FROM t6 WHERE v1 = v2;
SELECT * FROM t7 WHERE v1 = v2;
SELECT * FROM t8 WHERE v1 = v2;

SELECT * FROM t1 WHERE v1 <=> v2;
SELECT * FROM t2 WHERE v1 <=> v2;
SELECT * FROM t3 WHERE v1 <=> v2;

SELECT * FROM t1 WHERE v1 > v2;
SELECT * FROM t2 WHERE v1 > v2;
SELECT * FROM t3 WHERE v1 > v2;

SELECT * FROM t1 WHERE v1 IN (v2);
SELECT * FROM t2 WHERE v1 IN (v2);
SELECT * FROM t3 WHERE v1 IN (v2);

SELECT * FROM t1 WHERE v1 IN (map(2, 'b', 1, 'a'), map(1, 'a', 3, 'c'));
SELECT * FROM t2 WHERE v1 IN (array(map(2, 'b', 1, 'a')));
SELECT * FROM t3 WHERE v1 IN (struct(map(2, 'b', 1, 'a'), 'x'));

-- JOIN cases
SELECT * FROM t1 l, t1 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;
SELECT * FROM t2 l, t2 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;
SELECT * FROM t3 l, t3 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;
SELECT * FROM t4 l, t4 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;
SELECT * FROM t5 l, t5 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;
SELECT * FROM t6 l, t6 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;
SELECT * FROM t7 l, t7 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;
SELECT * FROM t8 l, t8 r WHERE l.v1 = r.v1 AND l.v2 = r.v2;

-- GROUP BY cases
SELECT v1, count(1) FROM t1 GROUP BY v1;
SELECT v1, count(1) FROM t2 GROUP BY v1;
SELECT v1, count(1) FROM t3 GROUP BY v1;
SELECT v1, count(1) FROM t4 GROUP BY v1;
SELECT v1, count(1) FROM t5 GROUP BY v1;
SELECT v1, count(1) FROM t6 GROUP BY v1;
SELECT v1, count(1) FROM t7 GROUP BY v1;
SELECT v1, count(1) FROM t8 GROUP BY v1;

-- WINDOW cases
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t1 ORDER BY v1;
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t2 ORDER BY v1;
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t3 ORDER BY v1;
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t4 ORDER BY v1;
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t5 ORDER BY v1;
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t6 ORDER BY v1;
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t7 ORDER BY v1;
SELECT v1, count(1) OVER(PARTITION BY v1) FROM t8 ORDER BY v1;

-- INTERSECT cases
(SELECT v1 FROM t1) INTERSECT (SELECT v1 FROM t1);
(SELECT v1 FROM t2) INTERSECT (SELECT v1 FROM t2);
(SELECT v1 FROM t3) INTERSECT (SELECT v1 FROM t3);
(SELECT v1 FROM t4) INTERSECT (SELECT v1 FROM t4);
(SELECT v1 FROM t5) INTERSECT (SELECT v1 FROM t5);
(SELECT v1 FROM t6) INTERSECT (SELECT v1 FROM t6);
(SELECT v1 FROM t7) INTERSECT (SELECT v1 FROM t7);
(SELECT v1 FROM t8) INTERSECT (SELECT v1 FROM t8);

-- EXCEPT cases
(SELECT v1 FROM t1) EXCEPT (SELECT v1 FROM t1 ORDER BY v1 LIMIT 1);
(SELECT v1 FROM t2) EXCEPT (SELECT v1 FROM t2 ORDER BY v1 LIMIT 1);
(SELECT v1 FROM t3) EXCEPT (SELECT v1 FROM t3 ORDER BY v1 LIMIT 1);
(SELECT v1 FROM t4) EXCEPT (SELECT v1 FROM t4 ORDER BY v1 LIMIT 1);
(SELECT v1 FROM t5) EXCEPT (SELECT v1 FROM t5 ORDER BY v1 LIMIT 1);
(SELECT v1 FROM t6) EXCEPT (SELECT v1 FROM t6 ORDER BY v1 LIMIT 1);
(SELECT v1 FROM t7) EXCEPT (SELECT v1 FROM t7 ORDER BY v1 LIMIT 1);
(SELECT v1 FROM t8) EXCEPT (SELECT v1 FROM t8 ORDER BY v1 LIMIT 1);

-- DISTINCT cases
SELECT DISTINCT v1 FROM t1;
SELECT DISTINCT v1 FROM t2;
SELECT DISTINCT v1 FROM t3;
SELECT DISTINCT v1 FROM t4;
SELECT DISTINCT v1 FROM t5;
SELECT DISTINCT v1 FROM t6;
SELECT DISTINCT v1 FROM t7;
SELECT DISTINCT v1 FROM t8;

-- To check if `SortMapKeys` inserted correctly, the explain results
-- of following complicated query cases are shown in `comparable-map-explain.sql`.

-- Combination tests of Sort/Filter/Join/Aggregate/Window + binary comparisons
SELECT * FROM t1 ORDER BY v1 = v2;
SELECT * FROM t1 WHERE v1 = v2 AND v1 = map_concat(v2, map(1, 'a'));
SELECT * FROM t1 l, t1 r WHERE l.v1 = r.v2 AND l.v1 = map_concat(r.v2, map(1, 'a'));
SELECT v1 = v2, count(1) FROM t1 GROUP BY v1 = v2;
SELECT v1 = v2, count(1) OVER(PARTITION BY v1 = v2) FROM t1 ORDER BY v1;

-- Combination tests of floating-point/map value normalization
CREATE TEMPORARY VIEW t9 AS SELECT * FROM VALUES
  (map("a", 0.0D, "b", -0.0D)), (map("b", 0.0D, "a", -0.0D))
AS t(v);

SELECT * FROM t9 l, t9 r WHERE l.v = r.v;
SELECT v, count(1) FROM t9 GROUP BY v;
SELECT v, count(1) OVER(PARTITION BY v) FROM t9 ORDER BY v;
