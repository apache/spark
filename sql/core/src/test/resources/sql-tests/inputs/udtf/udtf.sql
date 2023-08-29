CREATE OR REPLACE TEMPORARY VIEW t1 AS VALUES (0, 1), (1, 2) t(c1, c2);

-- test basic udtf
SELECT * FROM udtf(1, 2);
SELECT * FROM udtf(-1, 0);
SELECT * FROM udtf(0, -1);
SELECT * FROM udtf(0, 0);

-- test column alias
SELECT a, b FROM udtf(1, 2) t(a, b);

-- test lateral join
SELECT * FROM t1, LATERAL udtf(c1, c2);
SELECT * FROM t1 LEFT JOIN LATERAL udtf(c1, c2);
SELECT * FROM udtf(1, 2) t(c1, c2), LATERAL udtf(c1, c2);

-- test non-deterministic input
SELECT * FROM udtf(cast(rand(0) AS int) + 1, 1);
