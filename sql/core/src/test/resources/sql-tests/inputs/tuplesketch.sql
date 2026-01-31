-- Positive test cases
-- Create tables with key-value pairs for tuple sketches

-- Integer key with double values
DROP TABLE IF EXISTS t_int_double_1_5_through_7_11;
CREATE TABLE t_int_double_1_5_through_7_11 AS
VALUES
  (1, 1.0D, 5, 5.0D), (2, 2.0D, 6, 6.0D), (3, 3.0D, 7, 7.0D),
  (4, 4.0D, 8, 8.0D), (5, 5.0D, 9, 9.0D), (6, 6.0D, 10, 10.0D),
  (7, 7.0D, 11, 11.0D) AS tab(key1, val1, key2, val2);

-- Long key with double values
DROP TABLE IF EXISTS t_long_double_1_5_through_7_11;
CREATE TABLE t_long_double_1_5_through_7_11 AS
VALUES
  (1L, 1.0D, 5L, 5.00D), (2L, 2.00D, 6L, 6.00D), (3L, 3.00D, 7L, 7.00D),
  (4L, 4.0D, 8L, 8.00D), (5L, 5.00D, 9L, 9.00D), (6L, 6.00D, 10L, 10.00D),
  (7L, 7.0D, 11L, 11.00D) AS tab(key1, val1, key2, val2);

-- Double key with double values
DROP TABLE IF EXISTS t_double_double_1_1_1_4_through_1_5_1_8;
CREATE TABLE t_double_double_1_1_1_4_through_1_5_1_8 AS
SELECT CAST(key1 AS DOUBLE) AS key1, CAST(val1 AS DOUBLE) AS val1,
       CAST(key2 AS DOUBLE) AS key2, CAST(val2 AS DOUBLE) AS val2
FROM VALUES
  (1.1, 1.0, 1.4, 4.0), (1.2, 2.0, 1.5, 5.0), (1.3, 3.0, 1.6, 6.0),
  (1.4, 4.0, 1.7, 7.0), (1.5, 5.0, 1.8, 8.0) AS tab(key1, val1, key2, val2);

-- Float key with double values
DROP TABLE IF EXISTS t_float_double_1_1_1_4_through_1_5_1_8;
CREATE TABLE t_float_double_1_1_1_4_through_1_5_1_8 AS
SELECT CAST(key1 AS FLOAT) key1, CAST(val1 AS DOUBLE) AS val1,
       CAST(key2 AS FLOAT) key2, CAST(val2 AS DOUBLE) AS val2
FROM VALUES
  (1.1, 1.0, 1.4, 4.0), (1.2, 2.0, 1.5, 5.0), (1.3, 3.0, 1.6, 6.0),
  (1.4, 4.0, 1.7, 7.0), (1.5, 5.0, 1.8, 8.0) AS tab(key1, val1, key2, val2);

-- String key with double values
DROP TABLE IF EXISTS t_string_double_a_d_through_e_h;
CREATE TABLE t_string_double_a_d_through_e_h AS
VALUES
  ('a', 1.00D, 'd', 4.00D), ('b', 2.00D, 'e', 5.00D), ('c', 3.00D, 'f', 6.00D),
  ('d', 4.00D, 'g', 7.00D), ('e', 5.00D, 'h', 8.00D) AS tab(key1, val1, key2, val2);

-- Binary key with double values
DROP TABLE IF EXISTS t_binary_double_a_b_through_e_f;
CREATE TABLE t_binary_double_a_b_through_e_f AS
VALUES
  (X'A', 1.00D, X'B', 2.00D), (X'B', 2.00D, X'C', 3.00D), (X'C', 3.00D, X'D', 4.00D),
  (X'D', 4.00D, X'E', 5.00D), (X'E', 5.00D, X'F', 6.00D) AS tab(key1, val1, key2, val2);

-- Array Integer key with double values
DROP TABLE IF EXISTS t_array_int_double_1_3_through_4_6;
CREATE TABLE t_array_int_double_1_3_through_4_6 AS
VALUES
  (ARRAY(1), 1.00D, ARRAY(3), 3.00D),
  (ARRAY(2), 2.00D, ARRAY(4), 4.00D),
  (ARRAY(3), 3.00D, ARRAY(5), 5.00D),
  (ARRAY(4), 4.00D, ARRAY(6), 6.00D) AS tab(key1, val1, key2, val2);

-- Array Long key with double values
DROP TABLE IF EXISTS t_array_long_double_1_3_through_4_6;
CREATE TABLE t_array_long_double_1_3_through_4_6 AS
VALUES
  (ARRAY(1L), 1.00D, ARRAY(3L), 3.00D),
  (ARRAY(2L), 2.00D, ARRAY(4L), 4.00D),
  (ARRAY(3L), 3.00D, ARRAY(5L), 5.00D),
  (ARRAY(4L), 4.00D, ARRAY(6L), 6.00D) AS tab(key1, val1, key2, val2);

-- Integer key with integer values
DROP TABLE IF EXISTS t_int_int_1_5_through_7_11;
CREATE TABLE t_int_int_1_5_through_7_11 AS
VALUES
  (1, 1, 5, 5), (2, 2, 6, 6), (3, 3, 7, 7),
  (4, 4, 8, 8), (5, 5, 9, 9), (6, 6, 10, 10),
  (7, 7, 11, 11) AS tab(key1, val1, key2, val2);

DROP TABLE IF EXISTS t_string_collation;
CREATE TABLE t_string_collation AS
VALUES
  ('', 1.00D), ('  ', 2.00D), (CAST(X'C1' AS STRING), 3.00D), (CAST(X'80' AS STRING), 4.00D),
  ('\uFFFD', 5.00D), ('Å', 6.00D), ('å', 7.00D), ('a\u030A', 8.00D), ('Å ', 9.00D), ('å  ', 10.00D),
  ('a\u030A   ', 11.00D) AS tab(key1, val1);

-- Test basic tuple_sketch_agg_double with IntegerType key and double summary from table
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1)) AS result
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_integer with IntegerType key and integer summary from table
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key1, val1, 12)) AS result
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_agg_double with ArrayType(IntegerType) key and double summary from table
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_sketch_agg_double with ArrayType(LongType) key and double summary from table
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key2, val2))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_sketch_agg_double with BinaryType key and double summary from table
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_sketch_agg_double with DoubleType key and double summary from table
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_sketch_agg_double with FloatType key and double summary from table
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key2, val2))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_sketch_agg_double with IntegerType key and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 22))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with LongType key and double summary
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with StringType key and double summary
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_sketch_agg_double with explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_integer with explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key1, val1, 12))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_agg_double with all parameters including mode - sum (default)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode - min
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode - max
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'max'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode - alwaysone
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'alwaysone'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary_double to aggregate summary values - sum mode
SELECT tuple_sketch_summary_double(tuple_sketch_agg_double(key1, val1, 12, 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary_double to aggregate summary values - min mode
SELECT tuple_sketch_summary_double(tuple_sketch_agg_double(key1, val1, 12, 'min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary_double to aggregate summary values - max mode
SELECT tuple_sketch_summary_double(tuple_sketch_agg_double(key1, val1, 12, 'max'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary_double to aggregate summary values - alwaysone mode
SELECT tuple_sketch_summary_double(tuple_sketch_agg_double(key1, val1, 12, 'alwaysone'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary_integer with integer summary type
SELECT tuple_sketch_summary_integer(tuple_sketch_agg_integer(key1, val1, 12, 'sum'), 'sum')
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_theta_double to get theta value from double sketch
SELECT tuple_sketch_theta_double(tuple_sketch_agg_double(key1, val1))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_theta_double with LongType key
SELECT tuple_sketch_theta_double(tuple_sketch_agg_double(key1, val1))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_sketch_theta_double with StringType key
SELECT tuple_sketch_theta_double(tuple_sketch_agg_double(key1, val1))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_sketch_theta_integer to get theta value from integer sketch
SELECT tuple_sketch_theta_integer(tuple_sketch_agg_integer(key1, val1, 12))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_union_double function with IntegerType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_double function with LongType key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1, 15),
    tuple_sketch_agg_double(key2, val2), 15))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_union_double function with DoubleType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_union_double function with FloatType key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1, 6),
    tuple_sketch_agg_double(key2, val2, 15), 15))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_union_double function with StringType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_union_double function with BinaryType key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2, 20), 20))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_union_double function with ArrayType(IntegerType) key sketches
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_union_double function with ArrayType(LongType) key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2, 13), 13))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_union_double with lgNomEntries and mode parameters
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1, 12, 'sum'),
    tuple_sketch_agg_double(key2, val2, 12, 'sum'), 12, 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_intersection_double function with IntegerType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_intersection_double function with LongType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1, 5),
    tuple_sketch_agg_double(key2, val2, 12)))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_intersection_double function with DoubleType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_intersection_double function with FloatType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1, 5),
    tuple_sketch_agg_double(key2, val2)))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_intersection_double function with StringType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_intersection_double function with BinaryType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2, 22)))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_intersection_double function with ArrayType(IntegerType) key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_intersection_double function with ArrayType(LongType) key sketches
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2, 10)))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_intersection_double with mode parameter
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1, 12, 'min'),
    tuple_sketch_agg_double(key2, val2, 12, 'min'), 'min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_difference_double function with IntegerType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_difference_double function with LongType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2, 5)))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_difference_double function with DoubleType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_difference_double function with FloatType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1, 12),
    tuple_sketch_agg_double(key2, val2)))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_difference_double function with StringType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_difference_double function with BinaryType key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1, 6),
    tuple_sketch_agg_double(key2, val2, 8)))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_difference_double function with ArrayType(IntegerType) key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_difference_double function with ArrayType(LongType) key sketches
SELECT tuple_sketch_estimate_double(
  tuple_difference_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2, 4)))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_difference_integer with integer summary type
SELECT tuple_sketch_estimate_integer(
  tuple_difference_integer(
    tuple_sketch_agg_integer(key1, val1, 12, 'sum'),
    tuple_sketch_agg_integer(key2, val2, 12, 'sum')))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_union_agg_double with IntegerType key and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 15, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2, 20) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_union_agg_double with DoubleType key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 12, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_union_agg_double with StringType key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 14, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_string_double_a_d_through_e_h
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_string_double_a_d_through_e_h);

-- Test tuple_union_agg_double with LongType key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 10, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_long_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_long_double_1_5_through_7_11);

-- Test tuple_union_agg_double with FloatType keysketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 6, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_union_agg_double with BinaryType key sketches
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 12, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_binary_double_a_b_through_e_f
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_binary_double_a_b_through_e_f);

-- Test tuple_union_agg_double with ArrayType(IntegerType) key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 12, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_array_int_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_array_int_double_1_3_through_4_6);

-- Test tuple_union_agg_double with ArrayType(LongType) key sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 16, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_array_long_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_array_long_double_1_3_through_4_6);

-- Test tuple_union_agg_integer with integer summary type
SELECT tuple_sketch_estimate_integer(tuple_union_agg_integer(sketch, 12, 'sum'))
FROM (SELECT tuple_sketch_agg_integer(key1, val1, 12, 'sum') as sketch FROM t_int_int_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_integer(key2, val2, 12, 'sum') as sketch FROM t_int_int_1_5_through_7_11);

-- Test tuple_intersection_agg_double with IntegerType key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_intersection_agg_double with LongType key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_long_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_long_double_1_5_through_7_11);

-- Test tuple_intersection_agg_double with FloatType key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_intersection_agg_double with DoubleType key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_intersection_agg_double with StringType key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_string_double_a_d_through_e_h
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_string_double_a_d_through_e_h);

-- Test tuple_intersection_agg_double with BinaryType key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_binary_double_a_b_through_e_f
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_binary_double_a_b_through_e_f);

-- Test tuple_intersection_agg_double with ArrayType(IntegerType) key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_array_int_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_array_int_double_1_3_through_4_6);

-- Test tuple_intersection_agg_double with ArrayType(LongType) key sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_array_long_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_array_long_double_1_3_through_4_6);

-- Test tuple_intersection_agg_integer with integer summary type
SELECT tuple_sketch_estimate_integer(tuple_intersection_agg_integer(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_integer(key1, val1, 12, 'sum') as sketch FROM t_int_int_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_integer(key2, val2, 12, 'sum') as sketch FROM t_int_int_1_5_through_7_11);

-- Test tuple_sketch_agg_double with IntegerType key and null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (1, 1.0D), (null, 2.0D), (2, 2.0D), (null, 3.0D), (3, 3.0D) tab(key, val);

-- Test tuple_sketch_agg_double with StringType key and null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES ('test', 1.0D), (null, 2.0D), ('null', 3.0D), (null, 4.0D) tab(key, val);

-- Test tuple_sketch_agg_double with StringType key and Float summary with null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES ('test', 1.0F), (null, 2.0F), ('null', 3.0F), ('null', 4.0F) tab(key, val);

-- Test tuple_sketch_agg_double with LongType key yand null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (100L, 1.0D), (null, 2.0D), (200L, 3.0D), (null, 4.0D), (300L, 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with DoubleType key and null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(CAST(key AS DOUBLE), val))
FROM VALUES (1.1, 1.0D), (null, 2.0D), (2.2, 3.0D), (null, 4.0D), (3.3, 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with FloatType key and null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(CAST(key AS FLOAT), val))
FROM VALUES (1.5, 1.0D), (null, 2.0D), (2.5, 3.0D), (null, 4.0D), (3.5, 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with BinaryType key and null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (X'AA', 1.0D), (null, 2.0D), (X'BB', 3.0D), (null, 4.0D), (X'CC', 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with ArrayType(IntegerType) key and null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (ARRAY(1, 2), 1.0D), (null, 2.0D), (ARRAY(3, 4), 3.0D), (null, 4.0D), (ARRAY(5, 6), 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with ArrayType(LongType) key and null values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (ARRAY(10L, 20L), 1.0), (null, 2.0D), (ARRAY(30L, 40L), 3.0D), (null, 4.0D), (ARRAY(50L, 60L), 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with arrays containing null elements
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (ARRAY(1, null), 1.0), (ARRAY(1), 2.0D), (ARRAY(2, null, 3), 3.0D), (ARRAY(4), 4.0D) tab(key, val);

-- Test tuple_sketch_agg_double with arrays containing null elements (LongType)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (ARRAY(10L, null), 1.0D), (ARRAY(10L), 2.0D), (ARRAY(20L, null, 30L), 3.0D), (ARRAY(40L), 4.0D) tab(key, val);

-- Test tuple_sketch_agg_double with null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (1, 1.0D), (2, null), (3, 3.0D), (4, null), (5, 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with StringType key and null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES ('a', 1.0D), ('b', null), ('c', 3.0D), ('d', null) tab(key, val);

-- Test tuple_sketch_agg_double with LongType key and null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (100L, 1.0D), (200L, null), (300L, 3.0D), (400L, null), (500L, 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with DoubleType key and null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(CAST(key AS DOUBLE), val))
FROM VALUES (1.1, 1.0D), (2.2, null), (3.3, 3.0D), (4.4, null) tab(key, val);

-- Test tuple_sketch_agg_double with FloatType key and null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(CAST(key AS FLOAT), val))
FROM VALUES (1.5, 1.0D), (2.5, null), (3.5, 3.0D), (4.5, null) tab(key, val);

-- Test tuple_sketch_agg_double with BinaryType key and null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (X'AA', 1.0D), (X'BB', null), (X'CC', 3.0D), (X'DD', null) tab(key, val);

-- Test tuple_sketch_agg_double with ArrayType(IntegerType) key and null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (ARRAY(1, 2), 1.0D), (ARRAY(3, 4), null), (ARRAY(5, 6), 3.0D), (ARRAY(7, 8), null) tab(key, val);

-- Test tuple_sketch_agg_double with ArrayType(LongType) key and null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (ARRAY(10L, 20L), 1.0D), (ARRAY(30L, 40L), null), (ARRAY(50L, 60L), 3.0D) tab(key, val);

-- Test tuple_sketch_agg_integer with null summary values (nulls should be ignored)
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12))
FROM VALUES (1, 1), (2, null), (3, 3), (4, null), (5, 5) tab(key, val);

-- Test tuple_sketch_agg_double with both null keys and null summaries (all nulls should be ignored)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (1, 1.0D), (null, 2.0D), (3, null), (null, null), (5, 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with all-null keys (should return null or empty sketch)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (null, 1.0D), (null, 2.0D), (null, 3.0D) tab(key, val);

-- Test tuple_sketch_agg_double with all-null summaries (should return null or empty sketch)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (1, null), (2, null), (3, null) tab(key, val);

-- Test tuple_sketch_agg_double with all-null keys and summaries (should return null or empty sketch)
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (null, null), (null, null), (null, null) tab(key, val);

-- Test tuple_sketch_agg_integer with all-null keys (should return null or empty sketch)
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12))
FROM VALUES (null, 1), (null, 2), (null, 3) tab(key, val);

-- Test tuple_sketch_agg_integer with all-null summaries (should return null or empty sketch)
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12))
FROM VALUES (1, null), (2, null), (3, null) tab(key, val);

-- Test tuple_sketch_agg_integer with all-null keys and summaries (should return null or empty sketch)
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12))
FROM VALUES (null, null), (null, null), (null, null) tab(key, val);

-- Test tuple_sketch_summary_double with all-null summaries
SELECT tuple_sketch_summary_double(tuple_sketch_agg_double(key, val, 12, 'sum'))
FROM VALUES (1, null), (2, null), (3, null) tab(key, val);

-- Test tuple_sketch_summary_integer with all-null summaries
SELECT tuple_sketch_summary_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'), 'sum')
FROM VALUES (1, null), (2, null), (3, null) tab(key, val);

-- Test tuple_sketch_agg_double with empty arrays
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (ARRAY(), 1.0D), (ARRAY(1, 2), 2.0D), (ARRAY(), 3.0D), (ARRAY(3, 4), 4.0D) tab(key, val);

-- Test tuple_sketch_agg_double with empty strings
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES ('', 1.0D), ('a', 2.0D), ('', 3.0D), ('b', 4.0D), ('c', 5.0D) tab(key, val);

-- Test tuple_sketch_agg_double with empty binary data
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val))
FROM VALUES (X'', 1.0D), (X'01', 2.0D), (X'02', 3.0D), (X'03', 4.0D), (CAST('  ' AS BINARY), 5.0D), (X'e280', 6.0D), (X'c1', 7.0D), (X'c120', 8.0D) tab(key, val);

-- Test tuple_sketch_agg_double with collated string data
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1)) utf8_b FROM t_string_collation;
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1 COLLATE UTF8_LCASE, val1)) utf8_lc FROM t_string_collation;
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1 COLLATE UNICODE, val1)) unicode FROM t_string_collation;
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1 COLLATE UNICODE_CI, val1)) unicode_ci FROM t_string_collation;
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1 COLLATE UTF8_BINARY_RTRIM, val1)) utf8_b_rt FROM t_string_collation;
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1 COLLATE UTF8_LCASE_RTRIM, val1)) utf8_lc_rt FROM t_string_collation;
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1 COLLATE UNICODE_RTRIM, val1)) unicode_rt FROM t_string_collation;
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1 COLLATE UNICODE_CI_RTRIM, val1)) unicode_ci_rt FROM t_string_collation;

-- Test tuple_sketch_summary_double with empty sketch (no rows)
SELECT tuple_sketch_summary_double(tuple_sketch_agg_double(key, val))
FROM VALUES (1, 1.0D) tab(key, val) WHERE key > 100;

-- Test tuple_sketch_summary_integer with empty sketch (no rows)
SELECT tuple_sketch_summary_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'), 'sum')
FROM VALUES (1, 1) tab(key, val) WHERE key > 100;

-- Test tuple_sketch_agg_double with mode parameter in uppercase - SUM
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'SUM'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode parameter in mixed case - Sum
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'Sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode parameter in uppercase - MIN
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'MIN'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode parameter in mixed case - Min
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'Min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode parameter in uppercase - MAX
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'MAX'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode parameter in mixed case - Max
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'Max'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode parameter in uppercase - ALWAYSONE
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'ALWAYSONE'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with mode parameter in mixed case - AlwaysOne
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key1, val1, 12, 'AlwaysOne'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_integer with mode parameter in uppercase - SUM
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key1, val1, 12, 'SUM'))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_agg_integer with mode parameter in mixed case - Max
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key1, val1, 12, 'Max'))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_union_agg_double with mode parameter in uppercase - MAX
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 12, 'MAX'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_union_agg_double with mode parameter in mixed case - Min
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 12, 'Min'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11);

-- Test intersection of disjoint sketches (no common keys)
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM (SELECT 1 as key1, 1.0D as val1, 10 as key2, 10.0D as val2
      UNION ALL
      SELECT 2, 2.0D, 20, 20.0D
      UNION ALL
      SELECT 3, 3.0D, 30, 30.0D) tab;

-- Test tuple_union_double with empty sketch (union with empty result set)
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_int_double_1_5_through_7_11
WHERE key1 > 100;

-- Test tuple_intersection_double with empty sketch (intersection with empty result set)
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(
    tuple_sketch_agg_double(key1, val1),
    tuple_sketch_agg_double(key2, val2)))
FROM t_int_double_1_5_through_7_11
WHERE key1 > 100;

-- Test tuple_union_double with one non-empty and one empty sketch
WITH non_empty AS (
  SELECT tuple_sketch_agg_double(key1, val1) as sketch1 FROM t_int_double_1_5_through_7_11
),
empty_sketch AS (
  SELECT tuple_sketch_agg_double(key1, val1) as sketch2 FROM t_int_double_1_5_through_7_11 WHERE key1 > 100
)
SELECT tuple_sketch_estimate_double(
  tuple_union_double(sketch1, sketch2))
FROM non_empty, empty_sketch;

-- Test tuple_intersection_double with one non-empty and one empty sketch
WITH non_empty AS (
  SELECT tuple_sketch_agg_double(key1, val1) as sketch1 FROM t_int_double_1_5_through_7_11
),
empty_sketch AS (
  SELECT tuple_sketch_agg_double(key1, val1) as sketch2 FROM t_int_double_1_5_through_7_11 WHERE key1 > 100
)
SELECT tuple_sketch_estimate_double(
  tuple_intersection_double(sketch1, sketch2))
FROM non_empty, empty_sketch;

-- Test tuple_union_agg_double with empty sketches
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(sketch, 12, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11 WHERE key1 > 100
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11 WHERE key2 > 100);

-- Test tuple_intersection_agg_double with empty sketches
SELECT tuple_sketch_estimate_double(tuple_intersection_agg_double(sketch, 'sum'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11 WHERE key1 > 100
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11 WHERE key2 > 100);

-- Comprehensive test using all TupleSketch functions in a single query
WITH sketches AS (
  SELECT 'int_sketch' as sketch_type, tuple_sketch_agg_double(key1, val1, 12, 'sum') as sketch
  FROM t_int_double_1_5_through_7_11
  UNION ALL
  SELECT 'long_sketch' as sketch_type, tuple_sketch_agg_double(key1, val1, 15, 'sum') as sketch
  FROM t_long_double_1_5_through_7_11
  UNION ALL
  SELECT 'double_sketch' as sketch_type, tuple_sketch_agg_double(key1, val1, 10, 'sum') as sketch
  FROM t_double_double_1_1_1_4_through_1_5_1_8
  UNION ALL
  SELECT 'string_sketch' as sketch_type, tuple_sketch_agg_double(key1, val1, 14, 'sum') as sketch
  FROM t_string_double_a_d_through_e_h
),
union_result AS (
  SELECT tuple_union_agg_double(sketch, 16, 'sum') as union_sketch FROM sketches
),
individual_sketches AS (
  SELECT
    tuple_sketch_agg_double(key1, val1, 12, 'sum') as sketch1,
    tuple_sketch_agg_double(key2, val2, 12, 'sum') as sketch2
  FROM t_int_double_1_5_through_7_11
)
SELECT
  -- Basic estimate from union of all sketches
  tuple_sketch_estimate_double((SELECT union_sketch FROM union_result)) as union_estimate,
  -- Summary aggregation from union
  tuple_sketch_summary_double((SELECT union_sketch FROM union_result), 'sum') as union_summary,
  -- Union of two individual sketches
  tuple_sketch_estimate_double(tuple_union_double(sketch1, sketch2, 15, 'sum')) as binary_union_estimate,
  -- Intersection of two individual sketches
  tuple_sketch_estimate_double(tuple_intersection_double(sketch1, sketch2, 'sum')) as intersection_estimate,
  -- Difference of two individual sketches
  tuple_sketch_estimate_double(tuple_difference_double(sketch1, sketch2)) as difference_estimate
FROM individual_sketches;

-- Overflow tests for tuple_sketch_agg_integer and tuple_sketch_agg_double

-- Test tuple_sketch_agg_integer with values that cause integer overflow when summed (sum mode)
-- Testing sum of values that exceed INT max (2147483647)
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'))
FROM VALUES
  (1, 2147483647),
  (2, 1),
  (3, 100) AS tab(key, val);

-- Test tuple_sketch_agg_integer with negative overflow (sum mode)
-- Testing sum of values that go below INT min (-2147483648)
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'))
FROM VALUES
  (1, -2147483648),
  (2, -1),
  (3, -100) AS tab(key, val);

-- Test tuple_sketch_agg_integer with mixed positive and negative values near overflow boundaries
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'))
FROM VALUES
  (1, 2147483647),
  (2, -2147483648),
  (3, 1000000) AS tab(key, val);

-- Test tuple_sketch_agg_integer with max mode - should handle max integer value
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12, 'max'))
FROM VALUES
  (1, 2147483647),
  (2, 2147483646),
  (3, 100) AS tab(key, val);

-- Test tuple_sketch_agg_integer with min mode - should handle min integer value
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12, 'min'))
FROM VALUES
  (1, -2147483648),
  (2, -2147483647),
  (3, 100) AS tab(key, val);

-- Test tuple_sketch_agg_double with values that cause overflow to positive infinity
-- Testing sum of very large double values
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val, 12, 'sum'))
FROM VALUES
  (1, 1.7976931348623157E308),
  (2, 1.7976931348623157E308),
  (3, 1.0E308) AS tab(key, val);

-- Test tuple_sketch_agg_double with values that cause overflow to negative infinity
-- Testing sum of very large negative double values
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val, 12, 'sum'))
FROM VALUES
  (1, -1.7976931348623157E308),
  (2, -1.7976931348623157E308),
  (3, -1.0E308) AS tab(key, val);

-- Test tuple_sketch_agg_double with max mode using Double.MAX_VALUE
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val, 12, 'max'))
FROM VALUES
  (1, 1.7976931348623157E308),
  (2, 1.0E308),
  (3, 100.0D) AS tab(key, val);

-- Test tuple_sketch_agg_double with min mode using very large negative value
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val, 12, 'min'))
FROM VALUES
  (1, -1.7976931348623157E308),
  (2, -1.0E308),
  (3, -100.0D) AS tab(key, val);

-- Test tuple_sketch_agg_double with same key, values that overflow when summed
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key, val, 12, 'sum'))
FROM VALUES
  (1, 1.0E308),
  (1, 1.0E308),
  (1, 1.0E308) AS tab(key, val);

-- Test tuple_sketch_summary_integer with overflow in sum mode
SELECT tuple_sketch_summary_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'), 'sum')
FROM VALUES
  (1, 2147483647),
  (2, 1),
  (3, 100) AS tab(key, val);

-- Test tuple_sketch_agg_integer with same key where multiple values overflow when summed, then extract summary
-- Key 1 gets three INT_MAX values that overflow when summed
SELECT tuple_sketch_summary_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'), 'sum')
FROM VALUES
  (1, 2147483647),
  (1, 2147483647),
  (1, 2147483647),
  (2, 100),
  (3, 200) AS tab(key, val);

-- Test tuple_sketch_agg_integer with negative overflow on same key, then extract both estimate and summary
-- Key 1 gets three INT_MIN values that overflow when summed
SELECT
  tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key, val, 12, 'sum')) AS estimate,
  tuple_sketch_summary_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'), 'sum') AS summary
FROM VALUES
  (1, -2147483648),
  (1, -2147483648),
  (1, -2147483648),
  (2, -100),
  (3, -200) AS tab(key, val);

-- Test tuple_sketch_agg_integer with mixed overflow - positive and negative values for same key
SELECT tuple_sketch_summary_integer(tuple_sketch_agg_integer(key, val, 12, 'sum'), 'sum')
FROM VALUES
  (1, 2147483647),
  (1, 2147483647),
  (1, -2147483648),
  (2, 500) AS tab(key, val);

-- Test tuple_sketch_summary_double with overflow to infinity
SELECT tuple_sketch_summary_double(tuple_sketch_agg_double(key, val, 12, 'sum'), 'sum')
FROM VALUES
  (1, 1.7976931348623157E308),
  (2, 1.7976931348623157E308) AS tab(key, val);

-- Test tuple_union_integer with sketches containing values that overflow when combined
SELECT tuple_sketch_estimate_integer(
  tuple_union_integer(
    tuple_sketch_agg_integer(key1, val1, 12, 'sum'),
    tuple_sketch_agg_integer(key2, val2, 12, 'sum'), 12, 'sum'))
FROM VALUES
  (1, 2147483647, 1, 1000) AS tab(key1, val1, key2, val2);

-- Test tuple_union_double with sketches containing values that overflow to infinity when combined
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    tuple_sketch_agg_double(key1, val1, 12, 'sum'),
    tuple_sketch_agg_double(key2, val2, 12, 'sum'), 12, 'sum'))
FROM VALUES
  (1, 1.0E308, 1, 1.0E308) AS tab(key1, val1, key2, val2);

-- Named parameter tests for tuple sketch functions

-- Test tuple_sketch_agg_double with named parameters - only required params
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key => key1, summary => val1))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with named parameters - setting only lgNomEntries
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key => key1, summary => val1, lgNomEntries => 14))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with named parameters - setting only mode
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key => key1, summary => val1, mode => 'max'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with named parameters - setting both lgNomEntries and mode
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(key => key1, summary => val1, lgNomEntries => 10, mode => 'min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_double with named parameters - different order
SELECT tuple_sketch_estimate_double(tuple_sketch_agg_double(mode => 'max', lgNomEntries => 15, summary => val1, key => key1))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg_integer with named parameters - only required params
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key => key1, summary => val1))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_agg_integer with named parameters - setting only mode
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(key => key1, summary => val1, mode => 'max'))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_agg_integer with named parameters - different order
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_integer(lgNomEntries => 14, key => key1, mode => 'sum', summary => val1))
FROM t_int_int_1_5_through_7_11;

-- Test tuple_union_agg_double with named parameters - only required param
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(child => sketch))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_union_agg_double with named parameters - setting only lgNomEntries
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(child => sketch, lgNomEntries => 14))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_union_agg_double with named parameters - setting only mode
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(child => sketch, mode => 'max'))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_union_agg_double with named parameters - different order
SELECT tuple_sketch_estimate_double(tuple_union_agg_double(mode => 'min', lgNomEntries => 13, child => sketch))
FROM (SELECT tuple_sketch_agg_double(key1, val1) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_double(key2, val2) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_union_agg_integer with named parameters - setting only mode
SELECT tuple_sketch_estimate_integer(tuple_union_agg_integer(child => sketch, mode => 'max'))
FROM (SELECT tuple_sketch_agg_integer(key1, val1, 12, 'sum') as sketch FROM t_int_int_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg_integer(key2, val2, 12, 'sum') as sketch FROM t_int_int_1_5_through_7_11);

-- Test tuple_union_double with named parameters - only required params
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    first => tuple_sketch_agg_double(key1, val1),
    second => tuple_sketch_agg_double(key2, val2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_double with named parameters - setting only lgNomEntries
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    first => tuple_sketch_agg_double(key1, val1),
    second => tuple_sketch_agg_double(key2, val2),
    lgNomEntries => 14))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_double with named parameters - setting only mode
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    first => tuple_sketch_agg_double(key1, val1),
    second => tuple_sketch_agg_double(key2, val2),
    mode => 'max'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_double with named parameters - different order
SELECT tuple_sketch_estimate_double(
  tuple_union_double(
    mode => 'min',
    lgNomEntries => 15,
    second => tuple_sketch_agg_double(key2, val2),
    first => tuple_sketch_agg_double(key1, val1)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_integer with named parameters - setting only lgNomEntries
SELECT tuple_sketch_estimate_integer(
  tuple_union_integer(
    first => tuple_sketch_agg_integer(key1, val1),
    second => tuple_sketch_agg_integer(key2, val2),
    lgNomEntries => 14))
FROM t_int_int_1_5_through_7_11;

-- Negative test cases

-- Test tuple_sketch_agg_double with lgNomEntries value of 2 (too low, minimum is 4) - should fail
SELECT tuple_sketch_agg_double(col, val, 2)
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Test tuple_sketch_agg_double with lgNomEntries value of 40 (too high, maximum is 26) - should fail
SELECT tuple_sketch_agg_double(col, val, 40)
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Test tuple_union_agg_double with lgNomEntries value of 3 (too low, minimum is 4) - should fail
SELECT tuple_union_agg_double(sketch, 3, 'sum')
FROM (SELECT tuple_sketch_agg_double(col, val, 12) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val)
      UNION ALL
      SELECT tuple_sketch_agg_double(col, val, 20) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val));

-- Test tuple_union_agg_double with lgNomEntries value of 27 (too high, maximum is 26) - should fail
SELECT tuple_union_agg_double(sketch, 27, 'sum')
FROM (SELECT tuple_sketch_agg_double(col, val, 12) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val)
      UNION ALL
      SELECT tuple_sketch_agg_double(col, val, 20) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val));

-- lgNomEntries parameter is NULL
SELECT tuple_sketch_agg_double(col, val, CAST(NULL AS INT)) AS lg_nom_entries_is_null
FROM VALUES (15, 1.0D), (16, 2.0D), (17, 3.0D) tab(col, val);

-- lgNomEntries parameter is not foldable (non-constant)
SELECT tuple_sketch_agg_double(col, val, CAST(col AS INT)) AS lg_nom_entries_non_constant
FROM VALUES (15, 1.0D), (16, 2.0D), (17, 3.0D) tab(col, val);

-- lgNomEntries parameter has wrong type (STRING instead of INT)
SELECT tuple_sketch_agg_double(col, val, 'fifteen')
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- mode parameter is not foldable (non-constant)
SELECT tuple_sketch_agg_double(col, val, 12, CAST(col AS STRING))
FROM VALUES (15, 1.0D), (16, 2.0D), (17, 3.0D) tab(col, val);

-- Invalid mode parameter value
SELECT tuple_sketch_agg_double(col, val, 12, 'invalid_type')
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Invalid input - summary is a string when supposed to be double
SELECT tuple_sketch_agg_double(col, 'fifteen')
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Invalid input - double instead of int for lgNomEntries
SELECT tuple_sketch_agg_double(col, val, 2.0D, 15)
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Test tuple_union_double with integers (1, 2) instead of binary sketch data - should fail
SELECT tuple_union_double(1, 2)
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_intersection_double with integers (1, 2) instead of binary sketch data - should fail
SELECT tuple_intersection_double(1, 2)
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_difference_double with integers (1, 2) instead of binary sketch data - should fail
SELECT tuple_difference_double(1, 2)
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_union_double with string 'invalid' instead of integer for lgNomEntries parameter - should fail
SELECT tuple_union_double(
    tuple_sketch_agg_double(col1, 1.0D),
    tuple_sketch_agg_double(col2, 2.0D), 'invalid')
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_intersection_double with string 'invalid_sketch' instead of binary sketch data - should fail
SELECT tuple_intersection_double(
    tuple_sketch_agg_double(col1, 1.0),
    'invalid_sketch')
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_sketch_estimate_double with invalid binary data ('abc') that is not a valid tuple sketch - should fail
SELECT tuple_sketch_estimate_double(CAST('abc' AS BINARY));

-- Test tuple_union_double with invalid binary data ('abc', 'def') that are not valid tuple sketches - should fail
SELECT tuple_union_double(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test tuple_intersection_double with invalid binary data ('abc', 'def') that are not valid tuple sketches - should fail
SELECT tuple_intersection_double(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test tuple_difference_double with invalid binary data ('abc', 'def') that are not valid tuple sketches - should fail
SELECT tuple_difference_double(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test tuple_union_agg_double with invalid binary data ('abc') that is not a valid tuple sketch - should fail
SELECT tuple_union_agg_double(buffer, 15, 'sum')
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Test tuple_intersection_agg_double with invalid binary data ('abc') that is not a valid tuple sketch - should fail
SELECT tuple_intersection_agg_double(buffer, 'sum')
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Test tuple_sketch_agg_double with wrong value type (expecting double but got string)
SELECT tuple_sketch_agg_double(col1, col2, 12, 'sum')
FROM VALUES (1, 'invalid'), (2, 'invalid') AS tab(col1, col2);

-- Test tuple_sketch_agg_integer with wrong value type (expecting integer but got string)
SELECT tuple_sketch_agg_integer(col1, col2, 12, 'sum')
FROM VALUES (1, 'invalid'), (2, 'invalid') AS tab(col1, col2);

-- Test tuple_sketch_estimate_integer with wrong sketch type (integer estimate on double sketch)
SELECT tuple_sketch_estimate_integer(tuple_sketch_agg_double(key1, val1, 12, 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_theta_double with invalid binary data ('abc') that is not a valid tuple sketch - should fail
SELECT tuple_sketch_theta_double(CAST('abc' AS BINARY));

-- Test tuple_sketch_theta_integer with wrong sketch type (integer theta on double sketch) - should fail
SELECT tuple_sketch_theta_integer(tuple_sketch_agg_double(key1, val1, 12, 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_theta_double with integer input instead of binary - should fail
SELECT tuple_sketch_theta_double(123);

-- Clean up
DROP TABLE IF EXISTS t_int_double_1_5_through_7_11;
DROP TABLE IF EXISTS t_long_double_1_5_through_7_11;
DROP TABLE IF EXISTS t_double_double_1_1_1_4_through_1_5_1_8;
DROP TABLE IF EXISTS t_float_double_1_1_1_4_through_1_5_1_8;
DROP TABLE IF EXISTS t_string_double_a_d_through_e_h;
DROP TABLE IF EXISTS t_binary_double_a_b_through_e_f;
DROP TABLE IF EXISTS t_array_int_double_1_3_through_4_6;
DROP TABLE IF EXISTS t_array_long_double_1_3_through_4_6;
DROP TABLE IF EXISTS t_int_int_1_5_through_7_11;
DROP TABLE IF EXISTS t_string_collation;
