CREATE TEMPORARY VIEW various_maps AS SELECT * FROM VALUES (
  map(true, false),
  map(2Y, 1Y),
  map(2S, 1S),
  map(2, 1),
  map(2L, 1L),
  map(922337203685477897945456575809789456, 922337203685477897945456575809789456),
  map(9.22337203685477897945456575809789456, 9.22337203685477897945456575809789456),
  map(2.0D, 1.0D),
  map(float(2.0), float(1.0)),
  map(date '2016-03-14', date '2016-03-13'),
  map(timestamp '2016-11-15 20:54:00.000', timestamp '2016-11-12 20:54:00.000'),
  map('true', 'false', '2', '1'),
  map('2016-03-14', '2016-03-13'),
  map('2016-11-15 20:54:00.000', '2016-11-12 20:54:00.000'),
  map('922337203685477897945456575809789456', 'text'),
  map(array(1L, 2L), array(1L, 2L)), map(array(1, 2), array(1, 2)),
  map(struct(1S, 2L), struct(1S, 2L)), map(struct(1, 2), struct(1, 2)),
  map(float('NaN'), 1),
  map(float('NaN'), 2),
  map(float('Infinity'), 1),
  map(float('Infinity'), 2),
  map(float(0.0), 1),
  map(float(-0.0), 1)
) AS various_maps(
  boolean_map,
  tinyint_map,
  smallint_map,
  int_map,
  bigint_map,
  decimal_map1, decimal_map2,
  double_map,
  float_map,
  date_map,
  timestamp_map,
  string_map1, string_map2, string_map3, string_map4,
  array_map1, array_map2,
  struct_map1, struct_map2,
  float_nan_map1, float_nan_map2,
  float_infinity_map1, float_infinity_map2,
  float_zero_map, float_neg_zero_map
);

SELECT map_zip_with(tinyint_map, smallint_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(smallint_map, int_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(int_map, bigint_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(double_map, float_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(decimal_map1, decimal_map2, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(decimal_map1, int_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(decimal_map1, double_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(decimal_map2, int_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(decimal_map2, double_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(string_map1, int_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(string_map2, date_map, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(timestamp_map, string_map3, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(decimal_map1, string_map4, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(array_map1, array_map2, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(struct_map1, struct_map2, (k, v1, v2) -> struct(k, v1, v2)) m
FROM various_maps;

SELECT map_zip_with(float_nan_map1, float_nan_map2, (k, v1, v2) -> (v1, v2)) m
FROM various_maps;

SELECT map_zip_with(float_infinity_map1, float_infinity_map2, (k, v1, v2) -> (v1, v2)) m
FROM various_maps;

SELECT map_zip_with(float_zero_map, float_neg_zero_map, (k, v1, v2) -> (v1, v2)) m
FROM various_maps;

-- map_zip_with coerces input map key types with the session-mode variant before binding the lambda
SELECT typeof(map_zip_with(map(1, 0), map(CAST(2 AS FLOAT), 0), (k, v1, v2) -> k));

-- map_zip_with with non-coercible key types reports MAP_ZIP_WITH_DIFF_TYPES, not an internal error
SELECT map_zip_with(map(true, false), map(1, 0), (k, v1, v2) -> struct(k, v1, v2));
SELECT map_zip_with(map(1, 0), map(DATE '2020-01-01', 0), (k, v1, v2) -> struct(k, v1, v2));

-- Other coercible key types, including nested ones, and lambdas that return values
SELECT typeof(map_zip_with(map(1, 0), map(2L, 0), (k, v1, v2) -> k));
SELECT typeof(map_zip_with(map(1, 0), map(2.5, 0), (k, v1, v2) -> k));
SELECT typeof(map_zip_with(map(array(1), 0), map(array(2L), 0), (k, v1, v2) -> k));
SELECT typeof(map_zip_with(map(1, 'a'), map(2L, 1), (k, v1, v2) -> v1));
SELECT map_zip_with(map(1, 0), map(CAST(1 AS FLOAT), 1), (k, v1, v2) -> v1 + v2);

-- Non-coercible key types in the other argument order and for other types
SELECT map_zip_with(map(1, 0), map(true, false), (k, v1, v2) -> struct(k, v1, v2));
SELECT map_zip_with(map(true, 0), map(TIMESTAMP '2020-01-01 00:00:00', 0), (k, v1, v2) -> k);
SELECT map_zip_with(map(array(1), 0), map(1, 0), (k, v1, v2) -> k);

-- String and integer keys, with and without ANSI mode
SELECT typeof(map_zip_with(map(1, 0), map('2', 0), (k, v1, v2) -> k));
SET spark.sql.ansi.enabled=false;
SELECT typeof(map_zip_with(map(1, 0), map(CAST(2 AS FLOAT), 0), (k, v1, v2) -> k));
SELECT typeof(map_zip_with(map(1, 0), map('2', 0), (k, v1, v2) -> k));
SELECT map_zip_with(map(true, false), map(1, 0), (k, v1, v2) -> struct(k, v1, v2));
SET spark.sql.ansi.enabled=true;
