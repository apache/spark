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

-- String key with string values (for string summary type)
DROP TABLE IF EXISTS t_string_string_a_d_through_e_h;
CREATE TABLE t_string_string_a_d_through_e_h AS
VALUES
  ('a', 'val_a', 'd', 'val_d'), ('b', 'val_b', 'e', 'val_e'),
  ('c', 'val_c', 'f', 'val_f'), ('d', 'val_d', 'g', 'val_g'),
  ('e', 'val_e', 'h', 'val_h') AS tab(key1, val1, key2, val2);

-- Table with array values
DROP TABLE IF EXISTS t_string_array_string_a_d_through_e_h;
CREATE TABLE t_string_array_string_a_d_through_e_h AS
VALUES
  ('a', array('val_a', 'extra_a'), 'd', array('val_d')), 
  ('b', array('val_b'), 'e', array('val_e', 'extra_e')),
  ('c', array('val_c1', 'val_c2', 'val_c3'), 'f', array('val_f')), 
  ('d', array('val_d'), 'g', array('val_g')),
  ('e', array('val_e'), 'h', array('val_h1', 'val_h2')) 
AS tab(key1, val1, key2, val2);

DROP TABLE IF EXISTS t_string_collation;
CREATE TABLE t_string_collation AS
VALUES
  ('', 1.00D), ('  ', 2.00D), (CAST(X'C1' AS STRING), 3.00D), (CAST(X'80' AS STRING), 4.00D),
  ('\uFFFD', 5.00D), ('Å', 6.00D), ('å', 7.00D), ('a\u030A', 8.00D), ('Å ', 9.00D), ('å  ', 10.00D),
  ('a\u030A   ', 11.00D) AS tab(key1, val1);

-- Test basic tuple_sketch_agg with IntegerType key and DoubleType value from table
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1))) AS result
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg with IntegerType key and IntegerType value from table
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'integer'), 'integer') AS result
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_agg with ArrayType(IntegerType) key and double values from table
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1)))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_sketch_agg with ArrayType(LongType) key and double values from table
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key2, val2)))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_sketch_agg with BinaryType key and double values from table
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1)))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_sketch_agg with DoubleType key and double values from table
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1)))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_sketch_agg with FloatType key and double values from table
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key2, val2)))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_sketch_agg with IntegerType key and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 22))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg with LongType key and double values
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1)))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_sketch_agg with StringType key and double values
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1)))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_sketch_agg with explicit summaryType parameter - double
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'double'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg with explicit summaryType parameter - integer
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'integer'), 'integer')
FROM t_int_int_1_5_through_7_11;

-- Test tuple_sketch_agg with explicit summaryType parameter - string
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'string'), 'string')
FROM t_string_string_a_d_through_e_h;

-- Test tuple_sketch_agg with explicit summaryType parameter - string array
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'string'), 'string')
FROM t_string_array_string_a_d_through_e_h;

-- Test tuple_sketch_agg with all parameters including mode - sum (default)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum'), 'double')
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg with mode - min
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg with mode - max
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'max'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg with mode - alwaysone
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'alwaysone'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary to aggregate summary values - sum mode
SELECT tuple_sketch_summary(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary to aggregate summary values - min mode
SELECT tuple_sketch_summary(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary to aggregate summary values - max mode
SELECT tuple_sketch_summary(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'max'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary to aggregate summary values - alwaysone mode
SELECT tuple_sketch_summary(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'alwaysone'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_summary with integer summary type
SELECT tuple_sketch_summary(tuple_sketch_agg(struct(key1, val1), 12, 'integer', 'sum'), 'integer', 'sum')
FROM t_int_int_1_5_through_7_11;

-- Test tuple_union function with IntegerType sketches
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union function with LongType sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1), 15),
    tuple_sketch_agg(struct(key2, val2)), 15))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_union function with DoubleType sketches
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_union function with FloatType sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1), 6),
    tuple_sketch_agg(struct(key2, val2), 15), 15))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_union function with StringType sketches
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_union function with BinaryType sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2), 20), 20))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_union function with ArrayType(IntegerType) sketches
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_union function with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2), 13), 13))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_union with summaryType and mode parameters
SELECT tuple_sketch_estimate(
  tuple_union(
    tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum'),
    tuple_sketch_agg(struct(key2, val2), 12, 'double', 'sum'), 12, 'double', 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_theta to merge tuple sketch with theta sketch
SELECT tuple_sketch_estimate(
  tuple_union_theta(
    tuple_sketch_agg(struct(key1, val1)),
    theta_sketch_agg(key2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_theta with explicit parameters
SELECT tuple_sketch_estimate(
  tuple_union_theta(
    tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum'),
    theta_sketch_agg(key2, 12), 12, 'double', 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_intersection function with IntegerType sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_intersection function with LongType sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1), 5),
    tuple_sketch_agg(struct(key2, val2), 12)))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_intersection function with DoubleType sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_intersection function with FloatType sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1), 5),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_intersection function with StringType sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_intersection function with BinaryType sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2), 22)))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_intersection function with ArrayType(IntegerType) sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_intersection function with ArrayType(LongType) sketches
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2), 10)))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_intersection with summaryType and mode parameters
SELECT tuple_sketch_estimate(
  tuple_intersection(
    tuple_sketch_agg(struct(key1, val1), 12, 'double', 'min'),
    tuple_sketch_agg(struct(key2, val2), 12, 'double', 'min'), 'double', 'min'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_intersection_theta to intersect tuple sketch with theta sketch
SELECT tuple_sketch_estimate(
  tuple_intersection_theta(
    tuple_sketch_agg(struct(key1, val1)),
    theta_sketch_agg(key2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_intersection_theta with explicit parameters
SELECT tuple_sketch_estimate(
  tuple_intersection_theta(
    tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum'),
    theta_sketch_agg(key2, 12), 'double', 'sum'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_difference function with IntegerType sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_difference function with LongType sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2), 5)))
FROM t_long_double_1_5_through_7_11;

-- Test tuple_difference function with DoubleType sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_double_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_difference function with FloatType sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1), 12),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_float_double_1_1_1_4_through_1_5_1_8;

-- Test tuple_difference function with StringType sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_string_double_a_d_through_e_h;

-- Test tuple_difference function with BinaryType sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1), 6),
    tuple_sketch_agg(struct(key2, val2), 8)))
FROM t_binary_double_a_b_through_e_f;

-- Test tuple_difference function with ArrayType(IntegerType) sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2))))
FROM t_array_int_double_1_3_through_4_6;

-- Test tuple_difference function with ArrayType(LongType) sketches
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1)),
    tuple_sketch_agg(struct(key2, val2), 4)))
FROM t_array_long_double_1_3_through_4_6;

-- Test tuple_difference with summaryType parameter
SELECT tuple_sketch_estimate(
  tuple_difference(
    tuple_sketch_agg(struct(key1, val1), 12, 'integer', 'sum'),
    tuple_sketch_agg(struct(key2, val2), 12, 'integer', 'sum'), 'integer'), 'integer')
FROM t_int_int_1_5_through_7_11;

-- Test tuple_difference_theta to compute A-NOT-B with theta sketch
SELECT tuple_sketch_estimate(
  tuple_difference_theta(
    tuple_sketch_agg(struct(key1, val1)),
    theta_sketch_agg(key2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_difference_theta with explicit parameters
SELECT tuple_sketch_estimate(
  tuple_difference_theta(
    tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum'),
    theta_sketch_agg(key2, 12), 'double'))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_union_agg with IntegerType and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 15, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2), 20) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_union_agg with DoubleType sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 12, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_union_agg with StringType sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 14, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_string_double_a_d_through_e_h
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_string_double_a_d_through_e_h);

-- Test tuple_union_agg with LongType sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 10, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_long_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_long_double_1_5_through_7_11);

-- Test tuple_union_agg with FloatType sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 6, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_union_agg with BinaryType sketches
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 12, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_binary_double_a_b_through_e_f
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_binary_double_a_b_through_e_f);

-- Test tuple_union_agg with ArrayType(IntegerType) sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 12, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_array_int_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_array_int_double_1_3_through_4_6);

-- Test tuple_union_agg with ArrayType(LongType) sketches and explicit lgNomEntries parameter
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 16, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_array_long_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_array_long_double_1_3_through_4_6);

-- Test tuple_union_agg with integer summary type
SELECT tuple_sketch_estimate(tuple_union_agg(sketch, 12, 'integer', 'sum'), 'integer')
FROM (SELECT tuple_sketch_agg(struct(key1, val1), 12, 'integer', 'sum') as sketch FROM t_int_int_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2), 12, 'integer', 'sum') as sketch FROM t_int_int_1_5_through_7_11);

-- Test tuple_intersection_agg with IntegerType sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_int_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_int_double_1_5_through_7_11);

-- Test tuple_intersection_agg with LongType sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_long_double_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_long_double_1_5_through_7_11);

-- Test tuple_intersection_agg with FloatType sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_float_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_intersection_agg with DoubleType sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_double_double_1_1_1_4_through_1_5_1_8);

-- Test tuple_intersection_agg with StringType sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_string_double_a_d_through_e_h
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_string_double_a_d_through_e_h);

-- Test tuple_intersection_agg with BinaryType sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_binary_double_a_b_through_e_f
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_binary_double_a_b_through_e_f);

-- Test tuple_intersection_agg with ArrayType(IntegerType) sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_array_int_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_array_int_double_1_3_through_4_6);

-- Test tuple_intersection_agg with ArrayType(LongType) sketches
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'double', 'sum'))
FROM (SELECT tuple_sketch_agg(struct(key1, val1)) as sketch FROM t_array_long_double_1_3_through_4_6
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2)) as sketch FROM t_array_long_double_1_3_through_4_6);

-- Test tuple_intersection_agg with integer summary type
SELECT tuple_sketch_estimate(tuple_intersection_agg(sketch, 'integer', 'sum'), 'integer')
FROM (SELECT tuple_sketch_agg(struct(key1, val1), 12, 'integer', 'sum') as sketch FROM t_int_int_1_5_through_7_11
      UNION ALL
      SELECT tuple_sketch_agg(struct(key2, val2), 12, 'integer', 'sum') as sketch FROM t_int_int_1_5_through_7_11);

-- Test tuple_sketch_agg with IntegerType and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (1, 1.0D), (null, 2.0D), (2, 2.0D), (null, 3.0D), (3, 3.0D) tab(key, val);

-- Test tuple_sketch_agg with StringType and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES ('test', 1.0D), (null, 2.0D), ('null', 3.0D), (null, 4.0D) tab(key, val);

-- Test tuple_sketch_agg with LongType and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (100L, 1.0D), (null, 2.0D), (200L, 3.0D), (null, 4.0D), (300L, 5.0D) tab(key, val);

-- Test tuple_sketch_agg with DoubleType and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(CAST(key AS DOUBLE), val)))
FROM VALUES (1.1, 1.0D), (null, 2.0D), (2.2, 3.0D), (null, 4.0D), (3.3, 5.0D) tab(key, val);

-- Test tuple_sketch_agg with FloatType and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(CAST(key AS FLOAT), val)))
FROM VALUES (1.5, 1.0D), (null, 2.0D), (2.5, 3.0D), (null, 4.0D), (3.5, 5.0D) tab(key, val);

-- Test tuple_sketch_agg with BinaryType and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (X'AA', 1.0D), (null, 2.0D), (X'BB', 3.0D), (null, 4.0D), (X'CC', 5.0D) tab(key, val);

-- Test tuple_sketch_agg with ArrayType(IntegerType) and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (ARRAY(1, 2), 1.0D), (null, 2.0D), (ARRAY(3, 4), 3.0D), (null, 4.0D), (ARRAY(5, 6), 5.0D) tab(key, val);

-- Test tuple_sketch_agg with ArrayType(LongType) and null values (nulls should be ignored)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (ARRAY(10L, 20L), 1.0), (null, 2.0D), (ARRAY(30L, 40L), 3.0D), (null, 4.0D), (ARRAY(50L, 60L), 5.0D) tab(key, val);

-- Test tuple_sketch_agg with arrays containing null elements
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (ARRAY(1, null), 1.0), (ARRAY(1), 2.0D), (ARRAY(2, null, 3), 3.0D), (ARRAY(4), 4.0D) tab(key, val);

-- Test tuple_sketch_agg with arrays containing null elements (LongType)
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (ARRAY(10L, null), 1.0D), (ARRAY(10L), 2.0D), (ARRAY(20L, null, 30L), 3.0D), (ARRAY(40L), 4.0D) tab(key, val);

-- Test tuple_sketch_agg with empty arrays
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (ARRAY(), 1.0D), (ARRAY(1, 2), 2.0D), (ARRAY(), 3.0D), (ARRAY(3, 4), 4.0D) tab(key, val);

-- Test tuple_sketch_agg with empty strings
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES ('', 1.0D), ('a', 2.0D), ('', 3.0D), ('b', 4.0D), ('c', 5.0D) tab(key, val);

-- Test tuple_sketch_agg with empty binary data
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key, val)))
FROM VALUES (X'', 1.0D), (X'01', 2.0D), (X'02', 3.0D), (X'03', 4.0D), (CAST('  ' AS BINARY), 5.0D), (X'e280', 6.0D), (X'c1', 7.0D), (X'c120', 8.0D) tab(key, val);

-- Test tuple_sketch_agg with collated string data
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1))) utf8_b FROM t_string_collation;
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1 COLLATE UTF8_LCASE, val1))) utf8_lc FROM t_string_collation;
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1 COLLATE UNICODE, val1))) unicode FROM t_string_collation;
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1 COLLATE UNICODE_CI, val1))) unicode_ci FROM t_string_collation;
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1 COLLATE UTF8_BINARY_RTRIM, val1))) utf8_b_rt FROM t_string_collation;
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1 COLLATE UTF8_LCASE_RTRIM, val1))) utf8_lc_rt FROM t_string_collation;
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1 COLLATE UNICODE_RTRIM, val1))) unicode_rt FROM t_string_collation;
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1 COLLATE UNICODE_CI_RTRIM, val1))) unicode_ci_rt FROM t_string_collation;

-- Comprehensive test using all TupleSketch functions in a single query
WITH sketches AS (
  SELECT 'int_sketch' as sketch_type, tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum') as sketch
  FROM t_int_double_1_5_through_7_11
  UNION ALL
  SELECT 'long_sketch' as sketch_type, tuple_sketch_agg(struct(key1, val1), 15, 'double', 'sum') as sketch
  FROM t_long_double_1_5_through_7_11
  UNION ALL
  SELECT 'double_sketch' as sketch_type, tuple_sketch_agg(struct(key1, val1), 10, 'double', 'sum') as sketch
  FROM t_double_double_1_1_1_4_through_1_5_1_8
  UNION ALL
  SELECT 'string_sketch' as sketch_type, tuple_sketch_agg(struct(key1, val1), 14, 'double', 'sum') as sketch
  FROM t_string_double_a_d_through_e_h
),
union_result AS (
  SELECT tuple_union_agg(sketch, 16, 'double', 'sum') as union_sketch FROM sketches
),
individual_sketches AS (
  SELECT
    tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum') as sketch1,
    tuple_sketch_agg(struct(key2, val2), 12, 'double', 'sum') as sketch2
  FROM t_int_double_1_5_through_7_11
)
SELECT
  -- Basic estimate from union of all sketches
  tuple_sketch_estimate((SELECT union_sketch FROM union_result)) as union_estimate,
  -- Summary aggregation from union
  tuple_sketch_summary((SELECT union_sketch FROM union_result), 'double', 'sum') as union_summary,
  -- Union of two individual sketches
  tuple_sketch_estimate(tuple_union(sketch1, sketch2, 15, 'double', 'sum')) as binary_union_estimate,
  -- Intersection of two individual sketches
  tuple_sketch_estimate(tuple_intersection(sketch1, sketch2, 'double', 'sum')) as intersection_estimate,
  -- Difference of two individual sketches
  tuple_sketch_estimate(tuple_difference(sketch1, sketch2, 'double')) as difference_estimate
FROM individual_sketches;

-- Negative test cases

-- Test tuple_sketch_agg with lgNomEntries value of 2 (too low, minimum is 4) - should fail
SELECT tuple_sketch_agg(struct(col, val), 2)
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Test tuple_sketch_agg with lgNomEntries value of 40 (too high, maximum is 26) - should fail
SELECT tuple_sketch_agg(struct(col, val), 40)
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Test tuple_union_agg with lgNomEntries value of 3 (too low, minimum is 4) - should fail
SELECT tuple_union_agg(sketch, 3, 'double', 'sum')
FROM (SELECT tuple_sketch_agg(struct(col, val), 12) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val)
      UNION ALL
      SELECT tuple_sketch_agg(struct(col, val), 20) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val));

-- Test tuple_union_agg with lgNomEntries value of 27 (too high, maximum is 26) - should fail
SELECT tuple_union_agg(sketch, 27, 'double', 'sum')
FROM (SELECT tuple_sketch_agg(struct(col, val), 12) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val)
      UNION ALL
      SELECT tuple_sketch_agg(struct(col, val), 20) as sketch
        FROM VALUES (1, 1.0D) AS tab(col, val));

-- lgNomEntries parameter is NULL
SELECT tuple_sketch_agg(struct(col, val), CAST(NULL AS INT)) AS lg_nom_entries_is_null
FROM VALUES (15, 1.0D), (16, 2.0D), (17, 3.0D) tab(col, val);

-- lgNomEntries parameter is not foldable (non-constant)
SELECT tuple_sketch_agg(struct(col, val), CAST(col AS INT)) AS lg_nom_entries_non_constant
FROM VALUES (15, 1.0D), (16, 2.0D), (17, 3.0D) tab(col, val);

-- lgNomEntries parameter has wrong type (STRING instead of INT)
SELECT tuple_sketch_agg(struct(col, val), '15')
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- summaryType parameter is not foldable (non-constant)
SELECT tuple_sketch_agg(struct(col, val), 12, CAST(col AS STRING))
FROM VALUES (15, 1.0D), (16, 2.0D), (17, 3.0D) tab(col, val);

-- Invalid summaryType parameter value
SELECT tuple_sketch_agg(struct(col, val), 12, 'invalid_type')
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- mode parameter is not foldable (non-constant)
SELECT tuple_sketch_agg(struct(col, val), 12, 'double', CAST(col AS STRING))
FROM VALUES (15, 1.0D), (16, 2.0D), (17, 3.0D) tab(col, val);

-- Invalid mode parameter value
SELECT tuple_sketch_agg(struct(col, val), 12, 'double', 'invalid_mode')
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Invalid struct input - not a struct
SELECT tuple_sketch_agg(col, 15)
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Invalid struct input - struct # of elements is not 2
SELECT tuple_sketch_agg(struct(col, val, 2.0D), 15)
FROM VALUES (50, 1.0D), (60, 2.0D), (60, 3.0D) tab(col, val);

-- Test tuple_union with integers (1, 2) instead of binary sketch data - should fail
SELECT tuple_union(1, 2)
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_intersection with integers (1, 2) instead of binary sketch data - should fail
SELECT tuple_intersection(1, 2)
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_difference with integers (1, 2) instead of binary sketch data - should fail
SELECT tuple_difference(1, 2)
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_union with string 'invalid' instead of integer for lgNomEntries parameter - should fail
SELECT tuple_union(
    tuple_sketch_agg(struct(col1, 1.0D)),
    tuple_sketch_agg(struct(col2, 2.0D)), 'invalid')
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_intersection with string 'invalid_sketch' instead of binary sketch data - should fail
SELECT tuple_intersection(
    tuple_sketch_agg(struct(col1, 1.0)),
    'invalid_sketch')
FROM VALUES
  (1, 4),
  (2, 5),
  (3, 6) AS tab(col1, col2);

-- Test tuple_sketch_estimate with invalid binary data ('abc') that is not a valid tuple sketch - should fail
SELECT tuple_sketch_estimate(CAST('abc' AS BINARY));

-- Test tuple_union with invalid binary data ('abc', 'def') that are not valid tuple sketches - should fail
SELECT tuple_union(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test tuple_intersection with invalid binary data ('abc', 'def') that are not valid tuple sketches - should fail
SELECT tuple_intersection(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test tuple_difference with invalid binary data ('abc', 'def') that are not valid tuple sketches - should fail
SELECT tuple_difference(CAST('abc' AS BINARY), CAST('def' AS BINARY));

-- Test tuple_union_agg with invalid binary data ('abc') that is not a valid tuple sketch - should fail
SELECT tuple_union_agg(buffer, 15, 'double', 'sum')
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Test tuple_intersection_agg with invalid binary data ('abc') that is not a valid tuple sketch - should fail
SELECT tuple_intersection_agg(buffer, 'double', 'sum')
FROM (SELECT CAST('abc' AS BINARY) AS buffer);

-- Test tuple_sketch_summary with invalid summaryType for operation - should fail
SELECT tuple_sketch_summary(tuple_sketch_agg(struct(key1, val1), 12, 'string', 'sum'), 'string', 'sum')
FROM t_string_string_a_d_through_e_h;

-- Test tuple_union_theta with mismatched sketch types - theta sketch in first argument should fail
SELECT tuple_union_theta(
    theta_sketch_agg(key1),
    tuple_sketch_agg(struct(key2, val2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_intersection_theta with mismatched sketch types - theta sketch in first argument should fail
SELECT tuple_intersection_theta(
    theta_sketch_agg(key1),
    tuple_sketch_agg(struct(key2, val2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_difference_theta with mismatched sketch types - theta sketch in first argument should fail
SELECT tuple_difference_theta(
    theta_sketch_agg(key1),
    tuple_sketch_agg(struct(key2, val2)))
FROM t_int_double_1_5_through_7_11;

-- Test tuple_sketch_agg with wrong struct field type for summary (expecting double but got string)
SELECT tuple_sketch_agg(struct(col1, col2), 12, 'double', 'sum')
FROM VALUES (1, 'invalid'), (2, 'invalid') AS tab(col1, col2);

-- Test tuple_sketch_agg with wrong struct field type for summary (expecting integer but got double)
SELECT tuple_sketch_agg(struct(col1, col2), 12, 'integer', 'sum')
FROM VALUES (1, 1.5D), (2, 2.5D) AS tab(col1, col2);

-- Test tuple_sketch_estimate with wrong summaryType parameter
SELECT tuple_sketch_estimate(tuple_sketch_agg(struct(key1, val1), 12, 'double', 'sum'), 'integer')
FROM t_int_double_1_5_through_7_11;

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
DROP TABLE IF EXISTS t_string_string_a_d_through_e_h;
DROP TABLE IF EXISTS t_string_collation;
