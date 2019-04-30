-- Concatenate mixed inputs (output type is string)
SELECT (col1 || col2 || col3) col
FROM (
  SELECT
    id col1,
    string(id + 1) col2,
    encode(string(id + 2), 'utf-8') col3
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4) || col5) col
FROM (
  SELECT
    'prefix_' col1,
    id col2,
    string(id + 1) col3,
    encode(string(id + 2), 'utf-8') col4,
    CAST(id AS DOUBLE) col5
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4)) col
FROM (
  SELECT
    string(id) col1,
    string(id + 1) col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

-- turn on concatBinaryAsString
set spark.sql.function.concatBinaryAsString=true;

SELECT (col1 || col2) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2
  FROM range(10)
);

SELECT (col1 || col2 || col3 || col4) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4)) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

-- turn off concatBinaryAsString
set spark.sql.function.concatBinaryAsString=false;

-- Concatenate binary inputs (output type is binary)
SELECT (col1 || col2) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2
  FROM range(10)
);

SELECT (col1 || col2 || col3 || col4) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4)) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

CREATE TEMPORARY VIEW various_arrays AS SELECT * FROM VALUES (
  array(true, false), array(true),
  array(2Y, 1Y), array(3Y, 4Y),
  array(2S, 1S), array(3S, 4S),
  array(2, 1), array(3, 4),
  array(2L, 1L), array(3L, 4L),
  array(9223372036854775809, 9223372036854775808), array(9223372036854775808, 9223372036854775809),
  array(2.0D, 1.0D), array(3.0D, 4.0D),
  array(float(2.0), float(1.0)), array(float(3.0), float(4.0)),
  array(date '2016-03-14', date '2016-03-13'), array(date '2016-03-12', date '2016-03-11'),
  array(timestamp '2016-11-15 20:54:00.000', timestamp '2016-11-12 20:54:00.000'),
  array(timestamp '2016-11-11 20:54:00.000'),
  array('a', 'b'), array('c', 'd'),
  array(array('a', 'b'), array('c', 'd')), array(array('e'), array('f')),
  array(struct('a', 1), struct('b', 2)), array(struct('c', 3), struct('d', 4)),
  array(map('a', 1), map('b', 2)), array(map('c', 3), map('d', 4))
) AS various_arrays(
  boolean_array1, boolean_array2,
  tinyint_array1, tinyint_array2,
  smallint_array1, smallint_array2,
  int_array1, int_array2,
  bigint_array1, bigint_array2,
  decimal_array1, decimal_array2,
  double_array1, double_array2,
  float_array1, float_array2,
  date_array1, data_array2,
  timestamp_array1, timestamp_array2,
  string_array1, string_array2,
  array_array1, array_array2,
  struct_array1, struct_array2,
  map_array1, map_array2
);

-- Concatenate arrays of the same type
SELECT
    (boolean_array1 || boolean_array2) boolean_array,
    (tinyint_array1 || tinyint_array2) tinyint_array,
    (smallint_array1 || smallint_array2) smallint_array,
    (int_array1 || int_array2) int_array,
    (bigint_array1 || bigint_array2) bigint_array,
    (decimal_array1 || decimal_array2) decimal_array,
    (double_array1 || double_array2) double_array,
    (float_array1 || float_array2) float_array,
    (date_array1 || data_array2) data_array,
    (timestamp_array1 || timestamp_array2) timestamp_array,
    (string_array1 || string_array2) string_array,
    (array_array1 || array_array2) array_array,
    (struct_array1 || struct_array2) struct_array,
    (map_array1 || map_array2) map_array
FROM various_arrays;

-- Concatenate arrays of different types
SELECT
    (tinyint_array1 || smallint_array2) ts_array,
    (smallint_array1 || int_array2) si_array,
    (int_array1 || bigint_array2) ib_array,
    (bigint_array1 || decimal_array2) bd_array,
    (decimal_array1 || double_array2) dd_array,
    (double_array1 || float_array2) df_array,
    (string_array1 || data_array2) std_array,
    (timestamp_array1 || string_array2) tst_array,
    (string_array1 || int_array2) sti_array
FROM various_arrays;
