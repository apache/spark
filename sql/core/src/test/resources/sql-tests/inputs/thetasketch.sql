-- Positive test cases
-- Create a table with some testing data.
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 USING JSON AS VALUES (0), (1), (2), (2), (2), (3), (4) as tab(col);

SELECT theta_sketch_estimate(theta_sketch_agg(col)) AS result FROM t1;

SELECT theta_sketch_estimate(theta_sketch_agg(col, 12))
FROM VALUES (50), (60), (60), (60), (75), (100) tab(col);

SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES ('abc'), ('def'), ('abc'), ('ghi'), ('abc') tab(col);

SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (ARRAY(1, 2)), (ARRAY(3, 4)), (ARRAY(1, 2)) tab(col);

SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2)))
  FROM VALUES
    (1, 4),
    (1, 4),
    (2, 5),
    (2, 5),
    (3, 6) AS tab(col1, col2);

SELECT theta_sketch_estimate(theta_union_agg(sketch, 15))
    FROM (SELECT theta_sketch_agg(col) as sketch
            FROM VALUES (1) AS tab(col)
          UNION ALL
          SELECT theta_sketch_agg(col, 20) as sketch
            FROM VALUES (1) AS tab(col));

-- Test theta_intersection
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2)))
  FROM VALUES
    (1, 1),
    (2, 1),
    (3, 2),
    (3, 3) AS tab(col1, col2);

-- Test theta_difference  
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2)))
  FROM VALUES
    (1, 4),
    (2, 4),
    (3, 5),
    (4, 5) AS tab(col1, col2);

-- Test theta_intersection_agg
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 15))
    FROM (SELECT theta_sketch_agg(col) as sketch
            FROM VALUES (1), (2) AS tab(col)
          UNION ALL
          SELECT theta_sketch_agg(col) as sketch
            FROM VALUES (1), (3) AS tab(col)
          UNION ALL  
          SELECT theta_sketch_agg(col) as sketch
            FROM VALUES (1), (4) AS tab(col));

-- Test with explicit lgNomEntries parameters
SELECT theta_sketch_estimate(theta_union(
    theta_sketch_agg(col1, 12),
    theta_sketch_agg(col2, 12), 15))
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

SELECT theta_sketch_estimate(theta_intersection(
    theta_sketch_agg(col1, 12),
    theta_sketch_agg(col2, 12), 15))
  FROM VALUES
    (1, 1),
    (2, 2),
    (3, 4) AS tab(col1, col2);

SELECT theta_sketch_estimate(theta_difference(
    theta_sketch_agg(col1, 12),
    theta_sketch_agg(col2, 12), 15))
  FROM VALUES
    (1, 4),
    (2, 4),
    (3, 5) AS tab(col1, col2);

-- Negative test cases

-- lgNomEntries value of 2 is out of range (too low, minimum is 4)
SELECT theta_sketch_agg(col, 2)
FROM VALUES (50), (60), (60) tab(col);

-- lgNomEntries value of 40 is out of range (too high, maximum is 26)
SELECT theta_sketch_agg(col, 40)
FROM VALUES (50), (60), (60) tab(col);

-- lgNomEntries value of 3 is out of range (too low, minimum is 4)
SELECT theta_union_agg(sketch, 3)
FROM (SELECT theta_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT theta_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

-- lgNomEntries value of 27 is out of range (too high, maximum is 26)
SELECT theta_union_agg(sketch, 27)
FROM (SELECT theta_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT theta_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

-- lgNomEntries value of 3 is out of range (too low, minimum is 4)
SELECT theta_intersection_agg(sketch, 3)
FROM (SELECT theta_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT theta_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

-- Passing integers (1, 2) instead of binary sketch data
SELECT theta_union(1, 2)
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Passing integers (1, 2) instead of binary sketch data
SELECT theta_intersection(1, 2)
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Passing integers (1, 2) instead of binary sketch data
SELECT theta_difference(1, 2)
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Passing string 'invalid' instead of integer for lgNomEntries parameter
SELECT theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2), 'invalid')
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Passing string 'invalid_sketch' instead of binary sketch data
SELECT theta_intersection(
    theta_sketch_agg(col1),
    'invalid_sketch')
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Passing string 'invalid' instead of integer for lgNomEntries parameter
SELECT theta_intersection_agg(sketch, 'invalid')
FROM (SELECT theta_sketch_agg(col) as sketch
        FROM VALUES (1) AS tab(col));

-- Passing invalid binary data ('abc') that is not a valid theta sketch
SELECT theta_sketch_estimate(CAST('abc' AS BINARY));

-- Passing invalid binary data ('abc', 'def') that are not valid theta sketches
SELECT theta_union(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Passing invalid binary data ('abc', 'def') that are not valid theta sketches
SELECT theta_intersection(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Passing invalid binary data ('abc', 'def') that are not valid theta sketches
SELECT theta_difference(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Passing invalid binary data ('abc') that is not a valid theta sketch
SELECT theta_union_agg(buffer, 15)
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Passing invalid binary data ('abc') that is not a valid theta sketch
SELECT theta_intersection_agg(buffer, 15)
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Clean up
DROP TABLE IF EXISTS t1;