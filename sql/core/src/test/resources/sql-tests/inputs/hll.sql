-- Positive test cases
-- Create a table with some testing data.
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 USING JSON AS VALUES (0), (1), (2), (2), (2), (3), (4) as tab(col);

SELECT hll_sketch_estimate(hll_sketch_agg(col)) AS result FROM t1;

SELECT hll_sketch_estimate(hll_sketch_agg(col, 12))
FROM VALUES (50), (60), (60), (60), (75), (100) tab(col);

SELECT hll_sketch_estimate(hll_sketch_agg(col))
FROM VALUES ('abc'), ('def'), ('abc'), ('ghi'), ('abc') tab(col);

SELECT hll_sketch_estimate(
  hll_union(
    hll_sketch_agg(col1),
    hll_sketch_agg(col2)))
  FROM VALUES
    (1, 4),
    (1, 4),
    (2, 5),
    (2, 5),
    (3, 6) AS tab(col1, col2);

SELECT hll_sketch_estimate(hll_union_agg(sketch, true))
    FROM (SELECT hll_sketch_agg(col) as sketch
            FROM VALUES (1) AS tab(col)
          UNION ALL
          SELECT hll_sketch_agg(col, 20) as sketch
            FROM VALUES (1) AS tab(col));

-- Negative test cases
SELECT hll_sketch_agg(col)
FROM VALUES (ARRAY(1, 2)), (ARRAY(3, 4)) tab(col);

SELECT hll_sketch_agg(col, 2)
FROM VALUES (50), (60), (60) tab(col);

SELECT hll_sketch_agg(col, 40)
FROM VALUES (50), (60), (60) tab(col);

SELECT hll_union(
    hll_sketch_agg(col1, 12),
    hll_sketch_agg(col2, 13))
  FROM VALUES
    (1, 4),
    (1, 4),
    (2, 5),
    (2, 5),
    (3, 6) AS tab(col1, col2);

SELECT hll_union_agg(sketch, false)
FROM (SELECT hll_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT hll_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

SELECT hll_union(1, 2)
  FROM VALUES
    (1, 4),
    (1, 4),
    (2, 5),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- The HLL functions receive invalid buffers as inputs.
SELECT hll_sketch_estimate(CAST ('abc' AS BINARY));

SELECT hll_union(CAST ('abc' AS BINARY), CAST ('def' AS BINARY));

SELECT hll_union_agg(buffer, false)
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Clean up
DROP TABLE IF EXISTS t1;
