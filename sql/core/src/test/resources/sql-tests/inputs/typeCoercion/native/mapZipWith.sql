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
  map(struct(1S, 2L), struct(1S, 2L)), map(struct(1, 2), struct(1, 2))
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
  struct_map1, struct_map2
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
