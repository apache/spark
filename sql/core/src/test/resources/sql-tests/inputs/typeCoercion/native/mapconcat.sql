CREATE TEMPORARY VIEW various_maps AS SELECT * FROM VALUES (
  map(true, false), map(false, true),
  map(1Y, 2Y), map(3Y, 4Y),
  map(1S, 2S), map(3S, 4S),
  map(4, 6), map(7, 8),
  map(6L, 7L), map(8L, 9L),
  map(9223372036854775809, 9223372036854775808), map(9223372036854775808, 9223372036854775809),  
  map(1.0D, 2.0D), map(3.0D, 4.0D),
  map(float(1.0D), float(2.0D)), map(float(3.0D), float(4.0D)),
  map(date '2016-03-14', date '2016-03-13'), map(date '2016-03-12', date '2016-03-11'),
  map(timestamp '2016-11-15 20:54:00.000', timestamp '2016-11-12 20:54:00.000'),
  map(timestamp '2016-11-11 20:54:00.000', timestamp '2016-11-09 20:54:00.000'),
  map('a', 'b'), map('c', 'd'),
  map(array('a', 'b'), array('c', 'd')), map(array('e'), array('f')),
  map(struct('a', 1), struct('b', 2)), map(struct('c', 3), struct('d', 4)),
  map('a', 1), map('c', 2),
  map(1, 'a'), map(2, 'c')
) AS various_maps (
  boolean_map1, boolean_map2,
  tinyint_map1, tinyint_map2,
  smallint_map1, smallint_map2,
  int_map1, int_map2,
  bigint_map1, bigint_map2,
  decimal_map1, decimal_map2,
  double_map1, double_map2,
  float_map1, float_map2,
  date_map1, date_map2,
  timestamp_map1,
  timestamp_map2,
  string_map1, string_map2,
  array_map1, array_map2,
  struct_map1, struct_map2,
  string_int_map1, string_int_map2,
  int_string_map1, int_string_map2
);

-- Concatenate maps of the same type
SELECT
    map_concat(boolean_map1, boolean_map2) boolean_map,
    map_concat(tinyint_map1, tinyint_map2) tinyint_map,
    map_concat(smallint_map1, smallint_map2) smallint_map,
    map_concat(int_map1, int_map2) int_map,
    map_concat(bigint_map1, bigint_map2) bigint_map,
    map_concat(decimal_map1, decimal_map2) decimal_map,
    map_concat(float_map1, float_map2) float_map,
    map_concat(double_map1, double_map2) double_map,
    map_concat(date_map1, date_map2) date_map,
    map_concat(timestamp_map1, timestamp_map2) timestamp_map,
    map_concat(string_map1, string_map2) string_map,
    map_concat(array_map1, array_map2) array_map,
    map_concat(struct_map1, struct_map2) struct_map,
    map_concat(string_int_map1, string_int_map2) string_int_map,
    map_concat(int_string_map1, int_string_map2) int_string_map
FROM various_maps;

-- Concatenate maps of different types
SELECT
    map_concat(tinyint_map1, smallint_map2) ts_map,
    map_concat(smallint_map1, int_map2) si_map,
    map_concat(int_map1, bigint_map2) ib_map,
    map_concat(bigint_map1, decimal_map2) bd_map,
    map_concat(decimal_map1, float_map2) df_map,
    map_concat(string_map1, date_map2) std_map,
    map_concat(timestamp_map1, string_map2) tst_map,
    map_concat(string_map1, int_map2) sti_map,
    map_concat(int_string_map1, tinyint_map2) istt_map
FROM various_maps;

-- Concatenate map of incompatible types 1
SELECT
    map_concat(tinyint_map1, array_map1) tm_map
FROM various_maps;

-- Concatenate map of incompatible types 2
SELECT
    map_concat(boolean_map1, int_map2) bi_map
FROM various_maps;

-- Concatenate map of incompatible types 3
SELECT
    map_concat(int_map1, struct_map2) is_map
FROM various_maps;

-- Concatenate map of incompatible types 4
SELECT
    map_concat(struct_map1, array_map2) ma_map
FROM various_maps;

-- Concatenate map of incompatible types 5
SELECT
    map_concat(int_map1, array_map2) ms_map
FROM various_maps;
