-- Positive test cases
-- Create tables with two columns for each data type

-- Integer table
DROP TABLE IF EXISTS t_int;
CREATE TABLE t_int AS
SELECT * FROM VALUES 
  (13, 10), (1, 11), (14, 12), (2, 12), (2, 13), (3, 13), (4, 14) AS tab(col1, col2);

-- Long table  
DROP TABLE IF EXISTS t_long;
CREATE TABLE t_long AS
SELECT * FROM VALUES
  (203L, 200L), (101L, 201L), (102L, 202L), (202L, 202L), (103L, 203L) AS tab(col1, col2);

-- Double table
DROP TABLE IF EXISTS t_double;
CREATE TABLE t_double AS
SELECT CAST(col1 AS DOUBLE) AS col1, CAST(col2 AS DOUBLE) AS col2
FROM VALUES
  (2.4, 2.1), (1.2, 2.2), (1.3, 2.3), (2.2, 2.2), (1.4, 2.4) AS tab(col1, col2);

-- Float table (must cast, otherwise Spark will store DOUBLEs)
DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float AS
SELECT CAST(col1 AS FLOAT) col1, CAST(col1 AS FLOAT) col2
FROM VALUES
  (2.1, 2.1), (1.2, 2.2), (1.3, 2.3), (2.4, 2.2), (1.4, 2.4) AS tab(col1, col2);

-- String table
DROP TABLE IF EXISTS t_string;
CREATE TABLE t_string AS
SELECT * FROM VALUES
  ('a', 'x'), ('b', 'y'), ('x', 'z'), ('b', 'y'), ('x', 'w') AS tab(col1, col2);

-- Binary table
DROP TABLE IF EXISTS t_binary;
CREATE TABLE t_binary AS 
SELECT * FROM VALUES 
  (X'AA', X'BB'), (X'BB', X'DD'), (X'EE', X'FF'), (X'22', X'DD'), (X'11', X'22') AS tab(col1, col2);

-- Array Integer table
DROP TABLE IF EXISTS t_array_int;
CREATE TABLE t_array_int AS
SELECT * FROM VALUES 
  (ARRAY(50, 60), ARRAY(10, 20)), 
  (ARRAY(10, 20), ARRAY(30, 40)), 
  (ARRAY(5, 6), ARRAY(50, 60)), 
  (ARRAY(3, 4), ARRAY(30, 40)) AS tab(col1, col2);

-- Array Long table
DROP TABLE IF EXISTS t_array_long;
CREATE TABLE t_array_long AS
SELECT * FROM VALUES 
  (ARRAY(30L, 40L), ARRAY(10L, 20L)), 
  (ARRAY(10L, 20L), ARRAY(30L, 40L)), 
  (ARRAY(5L, 6L), ARRAY(50L, 60L)), 
  (ARRAY(3L, 4L), ARRAY(30L, 40L)) AS tab(col1, col2);

-- Test basic theta_sketch_agg with IntegerType from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) AS result FROM t_int;

-- Test theta_sketch_agg with ArrayType(IntegerType) values from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_array_int;

-- Test theta_sketch_agg with ArrayType(LongType) values from table  
SELECT theta_sketch_estimate(theta_sketch_agg(col2)) FROM t_array_long;

-- Test theta_sketch_agg with BinaryType values from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_binary;

-- Test theta_sketch_agg with DoubleType values from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_double;

-- Test theta_sketch_agg with FloatType values from table (promoted to Double internally)
SELECT theta_sketch_estimate(theta_sketch_agg(col2)) FROM t_float;

-- Test theta_sketch_agg with IntegerType and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_sketch_agg(col1, 22)) FROM t_int;

-- Test theta_sketch_agg with LongType values
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_long;

-- Test theta_sketch_agg with StringType values
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_string;

-- Test theta_union function with IntegerType sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_int;

-- Test theta_union function with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1, 15),
    theta_sketch_agg(col2))) FROM t_long;

-- Test theta_union function with DoubleType sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_double;

-- Test theta_union function with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1, 6),
    theta_sketch_agg(col2, 15), 15)) FROM t_float;

-- Test theta_union function with StringType sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_string;

-- Test theta_union function with BinaryType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2), 20)) FROM t_binary;

-- Test theta_union function with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_array_int;

-- Test theta_union function with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 13))) FROM t_array_long;

-- Test theta_intersection function with IntegerType sketches  
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_int;

-- Test theta_intersection function with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1, 5),
    theta_sketch_agg(col2, 12), 15)) FROM t_long;

-- Test theta_intersection function with DoubleType sketches
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_double;

-- Test theta_intersection function with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1, 5),
    theta_sketch_agg(col2))) FROM t_float;

-- Test theta_intersection function with StringType sketches
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_string;

-- Test theta_intersection function with BinaryType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2), 22)) FROM t_binary;

-- Test theta_intersection function with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_array_int;

-- Test theta_intersection function with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 10))) FROM t_array_long;

-- Test theta_difference function with IntegerType sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_int;

-- Test theta_difference function with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2), 15)) FROM t_long;

-- Test theta_difference function with DoubleType sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_double;

-- Test theta_difference function with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1, 12),
    theta_sketch_agg(col2))) FROM t_float;

-- Test theta_difference function with StringType sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_string;

-- Test theta_difference function with BinaryType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1, 6),
    theta_sketch_agg(col2, 8), 20)) FROM t_binary;

-- Test theta_difference function with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_array_int;

-- Test theta_difference function with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 4))) FROM t_array_long;

-- Test theta_union_agg with IntegerType and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 15))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_int
          UNION ALL
          SELECT theta_sketch_agg(col2, 20) as sketch FROM t_int);

-- Test theta_union_agg with DoubleType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 12))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_double
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_double);

-- Test theta_union_agg with StringType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 14))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_string
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_string);

-- Test theta_union_agg with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 10))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_long
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_long);

-- Test theta_union_agg with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 6))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_float
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_float);

-- Test theta_union_agg with BinaryType sketches
SELECT theta_sketch_estimate(theta_union_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_binary
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_binary);

-- Test theta_union_agg with ArrayType(IntegerType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 12))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_int
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_int);

-- Test theta_union_agg with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 16))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_long
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_long);

-- Test theta_intersection_agg with IntegerType and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 15))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_int
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_int);

-- Test theta_intersection_agg with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 10))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_long
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_long);

-- Test theta_intersection_agg with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 8))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_float
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_float);

-- Test theta_intersection_agg with DoubleType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 12))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_double
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_double);

-- Test theta_intersection_agg with StringType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 14))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_string
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_string);

-- Test theta_intersection_agg with BinaryType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 18))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_binary
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_binary);

-- Test theta_intersection_agg with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_int
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_int);

-- Test theta_intersection_agg with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_intersection_agg(sketch, 9))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_long
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_long);

-- Test theta_sketch_agg with IntegerType and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (1), (null), (2), (null), (3) tab(col);

-- Test theta_sketch_agg with StringType and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES ('test'), (null), ('null'), (null) tab(col);

-- Test theta_sketch_agg with LongType and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (100L), (null), (200L), (null), (300L) tab(col);

-- Test theta_sketch_agg with DoubleType and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(CAST(col AS DOUBLE)))
FROM VALUES (1.1), (null), (2.2), (null), (3.3) tab(col);

-- Test theta_sketch_agg with FloatType and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(CAST(col AS FLOAT)))
FROM VALUES (1.5), (null), (2.5), (null), (3.5) tab(col);

-- Test theta_sketch_agg with BinaryType and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (X'AA'), (null), (X'BB'), (null), (X'CC') tab(col);

-- Test theta_sketch_agg with ArrayType(IntegerType) and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (ARRAY(1, 2)), (null), (ARRAY(3, 4)), (null), (ARRAY(5, 6)) tab(col);

-- Test theta_sketch_agg with ArrayType(LongType) and null values (nulls should be ignored)
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (ARRAY(10L, 20L)), (null), (ARRAY(30L, 40L)), (null), (ARRAY(50L, 60L)) tab(col);

-- Comprehensive test using all ThetaSketch functions in a single query
-- This query demonstrates the full workflow: aggregation -> union -> intersection -> difference -> estimate
WITH sketches AS (
  SELECT 'int_sketch' as sketch_type, theta_sketch_agg(col1, 12) as sketch FROM t_int
  UNION ALL
  SELECT 'long_sketch' as sketch_type, theta_sketch_agg(col1, 15) as sketch FROM t_long
  UNION ALL
  SELECT 'double_sketch' as sketch_type, theta_sketch_agg(col1, 10) as sketch FROM t_double
  UNION ALL
  SELECT 'string_sketch' as sketch_type, theta_sketch_agg(col1, 14) as sketch FROM t_string
),
union_result AS (
  SELECT theta_union_agg(sketch, 16) as union_sketch FROM sketches
),
individual_sketches AS (
  SELECT theta_sketch_agg(col1, 12) as sketch1, theta_sketch_agg(col2, 12) as sketch2 FROM t_int
)
SELECT 
  -- Basic estimate from union of all sketches
  theta_sketch_estimate((SELECT union_sketch FROM union_result)) as union_estimate,
  -- Union of two individual sketches
  theta_sketch_estimate(theta_union(sketch1, sketch2, 15)) as binary_union_estimate,
  -- Intersection of two individual sketches
  theta_sketch_estimate(theta_intersection(sketch1, sketch2, 15)) as intersection_estimate,
  -- Difference of two individual sketches
  theta_sketch_estimate(theta_difference(sketch1, sketch2, 15)) as difference_estimate
FROM individual_sketches;

-- Negative test cases

-- Test theta_sketch_agg with lgNomEntries value of 2 (too low, minimum is 4) - should fail
SELECT theta_sketch_agg(col, 2)
FROM VALUES (50), (60), (60) tab(col);

-- Test theta_sketch_agg with lgNomEntries value of 40 (too high, maximum is 26) - should fail
SELECT theta_sketch_agg(col, 40)
FROM VALUES (50), (60), (60) tab(col);

-- Test theta_union_agg with lgNomEntries value of 3 (too low, minimum is 4) - should fail
SELECT theta_union_agg(sketch, 3)
FROM (SELECT theta_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT theta_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

-- Test theta_union_agg with lgNomEntries value of 27 (too high, maximum is 26) - should fail
SELECT theta_union_agg(sketch, 27)
FROM (SELECT theta_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT theta_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

-- Test theta_intersection_agg with lgNomEntries value of 3 (too low, minimum is 4) - should fail
SELECT theta_intersection_agg(sketch, 3)
FROM (SELECT theta_sketch_agg(col, 12) as sketch
        FROM VALUES (1) AS tab(col)
      UNION ALL
      SELECT theta_sketch_agg(col, 20) as sketch
        FROM VALUES (1) AS tab(col));

-- Test theta_union with integers (1, 2) instead of binary sketch data - should fail
SELECT theta_union(1, 2)
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Test theta_intersection with integers (1, 2) instead of binary sketch data - should fail
SELECT theta_intersection(1, 2)
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Test theta_difference with integers (1, 2) instead of binary sketch data - should fail
SELECT theta_difference(1, 2)
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Test theta_union with string 'invalid' instead of integer for lgNomEntries parameter - should fail
SELECT theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2), 'invalid')
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Test theta_intersection with string 'invalid_sketch' instead of binary sketch data - should fail
SELECT theta_intersection(
    theta_sketch_agg(col1),
    'invalid_sketch')
  FROM VALUES
    (1, 4),
    (2, 5),
    (3, 6) AS tab(col1, col2);

-- Test theta_intersection_agg with string 'invalid' instead of integer for lgNomEntries parameter - should fail
SELECT theta_intersection_agg(sketch, 'invalid')
FROM (SELECT theta_sketch_agg(col) as sketch
        FROM VALUES (1) AS tab(col));

-- Test theta_sketch_estimate with invalid binary data ('abc') that is not a valid theta sketch - should fail
SELECT theta_sketch_estimate(CAST('abc' AS BINARY));

-- Test theta_union with invalid binary data ('abc', 'def') that are not valid theta sketches - should fail
SELECT theta_union(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test theta_intersection with invalid binary data ('abc', 'def') that are not valid theta sketches - should fail
SELECT theta_intersection(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test theta_difference with invalid binary data ('abc', 'def') that are not valid theta sketches - should fail
SELECT theta_difference(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test theta_union_agg with invalid binary data ('abc') that is not a valid theta sketch - should fail
SELECT theta_union_agg(buffer, 15)
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Test theta_intersection_agg with invalid binary data ('abc') that is not a valid theta sketch - should fail
SELECT theta_intersection_agg(buffer, 15)
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Clean up
DROP TABLE IF EXISTS t_int;
DROP TABLE IF EXISTS t_long;
DROP TABLE IF EXISTS t_double;
DROP TABLE IF EXISTS t_float;
DROP TABLE IF EXISTS t_string;
DROP TABLE IF EXISTS t_binary;
DROP TABLE IF EXISTS t_array_int;
DROP TABLE IF EXISTS t_array_long;