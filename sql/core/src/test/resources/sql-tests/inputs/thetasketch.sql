-- Positive test cases
-- Create tables with two columns for each data type

-- Integer table
DROP TABLE IF EXISTS t_int_1_5_through_7_11;
CREATE TABLE t_int_1_5_through_7_11 AS
VALUES 
  (1, 5), (2, 6), (3, 7), (4, 8), (5, 9), (6, 10), (7, 11) AS tab(col1, col2);

-- Long table  
DROP TABLE IF EXISTS t_long_1_5_through_7_11;
CREATE TABLE t_long_1_5_through_7_11 AS
VALUES
  (1L, 5L), (2L, 6L), (3L, 7L), (4L, 8L), (5L, 9L), (6L, 10L), (7L, 11L) AS tab(col1, col2);

-- Double table
DROP TABLE IF EXISTS t_double_1_1_1_4_through_1_5_1_8;
CREATE TABLE t_double_1_1_1_4_through_1_5_1_8 AS
SELECT CAST(col1 AS DOUBLE) AS col1, CAST(col2 AS DOUBLE) AS col2
FROM VALUES
  (1.1, 1.4), (1.2, 1.5), (1.3, 1.6), (1.4, 1.7), (1.5, 1.8) AS tab(col1, col2);

-- Float table (must cast, otherwise Spark will store DOUBLEs)
DROP TABLE IF EXISTS t_float_1_1_1_4_through_1_5_1_8;
CREATE TABLE t_float_1_1_1_4_through_1_5_1_8 AS
SELECT CAST(col1 AS FLOAT) col1, CAST(col2 AS FLOAT) col2
FROM VALUES
  (1.1, 1.4), (1.2, 1.5), (1.3, 1.6), (1.4, 1.7), (1.5, 1.8) AS tab(col1, col2);

-- String table
DROP TABLE IF EXISTS t_string_a_d_through_e_h;
CREATE TABLE t_string_a_d_through_e_h AS
VALUES
  ('a', 'd'), ('b', 'e'), ('c', 'f'), ('d', 'g'), ('e', 'h') AS tab(col1, col2);

-- Binary table
DROP TABLE IF EXISTS t_binary_a_b_through_e_f;
CREATE TABLE t_binary_a_b_through_e_f AS 
VALUES 
  (X'A', X'B'), (X'B', X'C'), (X'C', X'D'), (X'D', X'E'), (X'E', X'F') AS tab(col1, col2);

-- Array Integer table
DROP TABLE IF EXISTS t_array_int_1_3_through_4_6;
CREATE TABLE t_array_int_1_3_through_4_6 AS
VALUES 
  (ARRAY(1), ARRAY(3)), 
  (ARRAY(2), ARRAY(4)), 
  (ARRAY(3), ARRAY(5)), 
  (ARRAY(4), ARRAY(6)) AS tab(col1, col2);

-- Array Long table
DROP TABLE IF EXISTS t_array_long_1_3_through_4_6;
CREATE TABLE t_array_long_1_3_through_4_6 AS
VALUES 
  (ARRAY(1L), ARRAY(3L)), 
  (ARRAY(2L), ARRAY(4L)), 
  (ARRAY(3L), ARRAY(5L)), 
  (ARRAY(4L), ARRAY(6L)) AS tab(col1, col2);

DROP TABLE IF EXISTS t_string_collation;
-- `\u030A` is the "combining ring above" Unicode character: https://www.compart.com/en/unicode/U+030A
-- `\uFFFD is the Unicode replacement character
-- `\xC1` is an invalid Unicode byte.
-- `\x80` is a Unicode continuation byte, that is it cannot be the first byte of a multi-byte UTF8 character.
-- All strings are different based on the UTF8_BINARY collation.
-- The first and second strings are equal for any collation with the RTRIM modifier, and equal to the empty string.
-- The last three strings are respectively equal to the next last three strings for any collation with the RTRIM modifier.
-- The strings "\xC1", "\x80" and "\uFFFD" are equal for all collations except UTF8_BINARY.
-- The (sub)strings `å` and `a\u030A` are equal for the UNICODE family of collations.
-- `å` is the lowercase version of `Å`.
CREATE TABLE t_string_collation AS
VALUES
  (''), ('  '), (CAST(X'C1' AS STRING)), (CAST(X'80' AS STRING)),
  ('\uFFFD'), ('Å'), ('å'), ('a\u030A'), ('Å '), ('å  '),
  ('a\u030A   ') AS tab(col1);

-- Test basic theta_sketch_agg with IntegerType from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) AS result FROM t_int_1_5_through_7_11;

-- Test theta_sketch_agg with ArrayType(IntegerType) values from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_array_int_1_3_through_4_6;

-- Test theta_sketch_agg with ArrayType(LongType) values from table  
SELECT theta_sketch_estimate(theta_sketch_agg(col2)) FROM t_array_long_1_3_through_4_6;

-- Test theta_sketch_agg with BinaryType values from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_binary_a_b_through_e_f;

-- Test theta_sketch_agg with DoubleType values from table
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_double_1_1_1_4_through_1_5_1_8;

-- Test theta_sketch_agg with FloatType values from table (promoted to Double internally)
SELECT theta_sketch_estimate(theta_sketch_agg(col2)) FROM t_float_1_1_1_4_through_1_5_1_8;

-- Test theta_sketch_agg with IntegerType and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_sketch_agg(col1, 22)) FROM t_int_1_5_through_7_11;

-- Test theta_sketch_agg with LongType values
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_long_1_5_through_7_11;

-- Test theta_sketch_agg with StringType values
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) FROM t_string_a_d_through_e_h;

-- Test theta_union function with IntegerType sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_int_1_5_through_7_11;

-- Test theta_union function with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1, 15),
    theta_sketch_agg(col2))) FROM t_long_1_5_through_7_11;

-- Test theta_union function with DoubleType sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_double_1_1_1_4_through_1_5_1_8;

-- Test theta_union function with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1, 6),
    theta_sketch_agg(col2, 15), 15)) FROM t_float_1_1_1_4_through_1_5_1_8;

-- Test theta_union function with StringType sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_string_a_d_through_e_h;

-- Test theta_union function with BinaryType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2), 20)) FROM t_binary_a_b_through_e_f;

-- Test theta_union function with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_array_int_1_3_through_4_6;

-- Test theta_union function with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_union(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 13))) FROM t_array_long_1_3_through_4_6;

-- Test theta_intersection function with IntegerType sketches  
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_int_1_5_through_7_11;

-- Test theta_intersection function with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1, 5),
    theta_sketch_agg(col2, 12))) FROM t_long_1_5_through_7_11;

-- Test theta_intersection function with DoubleType sketches
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_double_1_1_1_4_through_1_5_1_8;

-- Test theta_intersection function with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1, 5),
    theta_sketch_agg(col2))) FROM t_float_1_1_1_4_through_1_5_1_8;

-- Test theta_intersection function with StringType sketches
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_string_a_d_through_e_h;

-- Test theta_intersection function with BinaryType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 22))) FROM t_binary_a_b_through_e_f;

-- Test theta_intersection function with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_array_int_1_3_through_4_6;

-- Test theta_intersection function with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_intersection(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 10))) FROM t_array_long_1_3_through_4_6;

-- Test theta_difference function with IntegerType sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_int_1_5_through_7_11;

-- Test theta_difference function with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 5))) FROM t_long_1_5_through_7_11;

-- Test theta_difference function with DoubleType sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_double_1_1_1_4_through_1_5_1_8;

-- Test theta_difference function with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1, 12),
    theta_sketch_agg(col2))) FROM t_float_1_1_1_4_through_1_5_1_8;

-- Test theta_difference function with StringType sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_string_a_d_through_e_h;

-- Test theta_difference function with BinaryType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1, 6),
    theta_sketch_agg(col2, 8))) FROM t_binary_a_b_through_e_f;

-- Test theta_difference function with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2))) FROM t_array_int_1_3_through_4_6;

-- Test theta_difference function with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(
  theta_difference(
    theta_sketch_agg(col1),
    theta_sketch_agg(col2, 4))) FROM t_array_long_1_3_through_4_6;

-- Test theta_union_agg with IntegerType and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 15))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_int_1_5_through_7_11
          UNION ALL
          SELECT theta_sketch_agg(col2, 20) as sketch FROM t_int_1_5_through_7_11);

-- Test theta_union_agg with DoubleType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 12))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_double_1_1_1_4_through_1_5_1_8
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_double_1_1_1_4_through_1_5_1_8);

-- Test theta_union_agg with StringType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 14))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_string_a_d_through_e_h
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_string_a_d_through_e_h);

-- Test theta_union_agg with LongType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 10))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_long_1_5_through_7_11
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_long_1_5_through_7_11);

-- Test theta_union_agg with FloatType sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 6))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_float_1_1_1_4_through_1_5_1_8
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_float_1_1_1_4_through_1_5_1_8);

-- Test theta_union_agg with BinaryType sketches
SELECT theta_sketch_estimate(theta_union_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_binary_a_b_through_e_f
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_binary_a_b_through_e_f);

-- Test theta_union_agg with ArrayType(IntegerType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 12))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_int_1_3_through_4_6
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_int_1_3_through_4_6);

-- Test theta_union_agg with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT theta_sketch_estimate(theta_union_agg(sketch, 16))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_long_1_3_through_4_6
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_long_1_3_through_4_6);

-- Test theta_intersection_agg with IntegerType sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_int_1_5_through_7_11
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_int_1_5_through_7_11);

-- Test theta_intersection_agg with LongType sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_long_1_5_through_7_11
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_long_1_5_through_7_11);

-- Test theta_intersection_agg with FloatType sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_float_1_1_1_4_through_1_5_1_8
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_float_1_1_1_4_through_1_5_1_8);

-- Test theta_intersection_agg with DoubleType sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_double_1_1_1_4_through_1_5_1_8
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_double_1_1_1_4_through_1_5_1_8);

-- Test theta_intersection_agg with StringType sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_string_a_d_through_e_h
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_string_a_d_through_e_h);

-- Test theta_intersection_agg with BinaryType sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_binary_a_b_through_e_f
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_binary_a_b_through_e_f);

-- Test theta_intersection_agg with ArrayType(IntegerType) sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_int_1_3_through_4_6
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_int_1_3_through_4_6);

-- Test theta_intersection_agg with ArrayType(LongType) sketches
SELECT theta_sketch_estimate(theta_intersection_agg(sketch))
    FROM (SELECT theta_sketch_agg(col1) as sketch FROM t_array_long_1_3_through_4_6
          UNION ALL
          SELECT theta_sketch_agg(col2) as sketch FROM t_array_long_1_3_through_4_6);

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

-- Test theta_sketch_agg with arrays containing null elements
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (ARRAY(1, null)), (ARRAY(1)), (ARRAY(2, null, 3)), (ARRAY(4)) tab(col);

-- Test theta_sketch_agg with arrays containing null elements (LongType)
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (ARRAY(10L, null)), (ARRAY(10L)), (ARRAY(20L, null, 30L)), (ARRAY(40L)) tab(col);

-- Test theta_sketch_agg with empty arrays
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (ARRAY()), (ARRAY(1, 2)), (ARRAY()), (ARRAY(3, 4)) tab(col);

-- Test theta_sketch_agg with empty strings
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (''), ('a'), (''), ('b'), ('c') tab(col);

-- Test theta_sketch_agg with empty binary data
SELECT theta_sketch_estimate(theta_sketch_agg(col))
FROM VALUES (X''), (X'01'), (X'02'), (X'03'), (CAST('  ' AS BINARY)), (X'e280'), (X'c1'), (X'c120') tab(col);

-- Test theta_sketch_agg with collated string data
SELECT theta_sketch_estimate(theta_sketch_agg(col1)) utf8_b FROM t_string_collation;
SELECT theta_sketch_estimate(theta_sketch_agg(col1 COLLATE UTF8_LCASE)) utf8_lc FROM t_string_collation;
SELECT theta_sketch_estimate(theta_sketch_agg(col1 COLLATE UNICODE)) unicode FROM t_string_collation;
SELECT theta_sketch_estimate(theta_sketch_agg(col1 COLLATE UNICODE_CI)) unicode_ci FROM t_string_collation;
SELECT theta_sketch_estimate(theta_sketch_agg(col1 COLLATE UTF8_BINARY_RTRIM)) utf8_b_rt FROM t_string_collation;
SELECT theta_sketch_estimate(theta_sketch_agg(col1 COLLATE UTF8_LCASE_RTRIM)) utf8_lc_rt FROM t_string_collation;
SELECT theta_sketch_estimate(theta_sketch_agg(col1 COLLATE UNICODE_RTRIM)) unicode_rt FROM t_string_collation;
SELECT theta_sketch_estimate(theta_sketch_agg(col1 COLLATE UNICODE_CI_RTRIM)) unicode_ci_rt FROM t_string_collation;

-- Comprehensive test using all ThetaSketch functions in a single query
-- This query demonstrates the full workflow: aggregation -> union -> intersection -> difference -> estimate
WITH sketches AS (
  SELECT 'int_sketch' as sketch_type, theta_sketch_agg(col1, 12) as sketch FROM t_int_1_5_through_7_11
  UNION ALL
  SELECT 'long_sketch' as sketch_type, theta_sketch_agg(col1, 15) as sketch FROM t_long_1_5_through_7_11
  UNION ALL
  SELECT 'double_sketch' as sketch_type, theta_sketch_agg(col1, 10) as sketch FROM t_double_1_1_1_4_through_1_5_1_8
  UNION ALL
  SELECT 'string_sketch' as sketch_type, theta_sketch_agg(col1, 14) as sketch FROM t_string_a_d_through_e_h
),
union_result AS (
  SELECT theta_union_agg(sketch, 16) as union_sketch FROM sketches
),
individual_sketches AS (
  SELECT theta_sketch_agg(col1, 12) as sketch1, theta_sketch_agg(col2, 12) as sketch2 FROM t_int_1_5_through_7_11
)
SELECT 
  -- Basic estimate from union of all sketches
  theta_sketch_estimate((SELECT union_sketch FROM union_result)) as union_estimate,
  -- Union of two individual sketches
  theta_sketch_estimate(theta_union(sketch1, sketch2, 15)) as binary_union_estimate,
  -- Intersection of two individual sketches
  theta_sketch_estimate(theta_intersection(sketch1, sketch2)) as intersection_estimate,
  -- Difference of two individual sketches
  theta_sketch_estimate(theta_difference(sketch1, sketch2)) as difference_estimate
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

-- lgNomEntries parameter is NULL
SELECT theta_sketch_agg(col, CAST(NULL AS INT)) AS lg_nom_entries_is_null
FROM VALUES (15), (16), (17) tab(col);

-- lgNomEntries parameter is not foldable (non-constant)
SELECT theta_sketch_agg(col, CAST(col AS INT)) AS lg_nom_entries_non_constant
FROM VALUES (15), (16), (17) tab(col);

-- lgNomEntries parameter has wrong type (STRING instead of INT)
SELECT theta_sketch_agg(col, '15')
FROM VALUES (50), (60), (60) tab(col);

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
SELECT theta_intersection_agg(buffer)
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Clean up
DROP TABLE IF EXISTS t_int_1_5_through_7_11;
DROP TABLE IF EXISTS t_long_1_5_through_7_11;
DROP TABLE IF EXISTS t_double_1_1_1_4_through_1_5_1_8;
DROP TABLE IF EXISTS t_float_1_1_1_4_through_1_5_1_8;
DROP TABLE IF EXISTS t_string_a_d_through_e_h;
DROP TABLE IF EXISTS t_binary_a_b_through_e_f;
DROP TABLE IF EXISTS t_array_int_1_3_through_4_6;
DROP TABLE IF EXISTS t_array_long_1_3_through_4_6;
DROP TABLE IF EXISTS t_string_collation