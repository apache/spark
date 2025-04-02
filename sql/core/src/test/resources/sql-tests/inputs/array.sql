-- test cases for array functions

create temporary view data as select * from values
  ("one", array(11, 12, 13), array(array(111, 112, 113), array(121, 122, 123))),
  ("two", array(21, 22, 23), array(array(211, 212, 213), array(221, 222, 223)))
  as data(a, b, c);

select * from data;

-- index into array
select a, b[0], b[0] + b[1] from data;

-- index into array of arrays
select a, c[0][0] + c[0][0 + 1] from data;


create temporary view primitive_arrays as select * from values (
  array(true),
  array(2Y, 1Y),
  array(2S, 1S),
  array(2, 1),
  array(2L, 1L),
  array(9223372036854775809, 9223372036854775808),
  array(2.0D, 1.0D),
  array(float(2.0), float(1.0)),
  array(date '2016-03-14', date '2016-03-13'),
  array(timestamp '2016-11-15 20:54:00.000',  timestamp '2016-11-12 20:54:00.000')
) as primitive_arrays(
  boolean_array,
  tinyint_array,
  smallint_array,
  int_array,
  bigint_array,
  decimal_array,
  double_array,
  float_array,
  date_array,
  timestamp_array
);

select * from primitive_arrays;

-- array_contains on all primitive types: result should alternate between true and false
select
  array_contains(boolean_array, true), array_contains(boolean_array, false),
  array_contains(tinyint_array, 2Y), array_contains(tinyint_array, 0Y),
  array_contains(smallint_array, 2S), array_contains(smallint_array, 0S),
  array_contains(int_array, 2), array_contains(int_array, 0),
  array_contains(bigint_array, 2L), array_contains(bigint_array, 0L),
  array_contains(decimal_array, 9223372036854775809), array_contains(decimal_array, 1),
  array_contains(double_array, 2.0D), array_contains(double_array, 0.0D),
  array_contains(float_array, float(2.0)), array_contains(float_array, float(0.0)),
  array_contains(date_array, date '2016-03-14'), array_contains(date_array, date '2016-01-01'),
  array_contains(timestamp_array, timestamp '2016-11-15 20:54:00.000'), array_contains(timestamp_array, timestamp '2016-01-01 20:54:00.000')
from primitive_arrays;

-- array_contains on nested arrays
select array_contains(b, 11), array_contains(c, array(111, 112, 113)) from data;

-- sort_array
select
  sort_array(boolean_array),
  sort_array(tinyint_array),
  sort_array(smallint_array),
  sort_array(int_array),
  sort_array(bigint_array),
  sort_array(decimal_array),
  sort_array(double_array),
  sort_array(float_array),
  sort_array(date_array),
  sort_array(timestamp_array)
from primitive_arrays;

-- sort_array with an invalid string literal for the argument of sort order.
select sort_array(array('b', 'd'), '1');

-- sort_array with an invalid null literal casted as boolean for the argument of sort order.
select sort_array(array('b', 'd'), cast(NULL as boolean));

-- size
select
  size(boolean_array),
  size(tinyint_array),
  size(smallint_array),
  size(int_array),
  size(bigint_array),
  size(decimal_array),
  size(double_array),
  size(float_array),
  size(date_array),
  size(timestamp_array)
from primitive_arrays;

-- index out of range for array elements
select element_at(array(1, 2, 3), 5);
select element_at(array(1, 2, 3), -5);
select element_at(array(1, 2, 3), 0);

select elt(4, '123', '456');
select elt(0, '123', '456');
select elt(-1, '123', '456');
select elt(null, '123', '456');
select elt(null, '123', null);
select elt(1, '123', null);
select elt(2, '123', null);

select array(1, 2, 3)[5];
select array(1, 2, 3)[-1];

-- array_size
select array_size(array());
select array_size(array(true));
select array_size(array(2, 1));
select array_size(NULL);
select array_size(map('a', 1, 'b', 2));

-- size(arrays_zip)
select size(arrays_zip(array(1, 2, 3), array(4), array(7, 8, 9, 10)));
select size(arrays_zip(array(), array(1, 2, 3), array(4), array(7, 8, 9, 10)));
select size(arrays_zip(array(1, 2, 3), array(4), null, array(7, 8, 9, 10)));

-- isnotnull(arrays_zip)
select isnotnull(arrays_zip(array(), array(4), array(7, 8, 9, 10)));
select isnotnull(arrays_zip(array(1, 2, 3), array(4), array(7, 8, 9, 10)));
select isnotnull(arrays_zip(array(1, 2, 3), NULL, array(4), array(7, 8, 9, 10)));

-- function get()
select get(array(1, 2, 3), 0);
select get(array(1, 2, 3), 3);
select get(array(1, 2, 3), null);
select get(array(1, 2, 3), -1);

-- function array_insert()
select array_insert(array(1, 2, 3), 3, 4);
select array_insert(array(2, 3, 4), 0, 1);
select array_insert(array(2, 3, 4), 1, 1);
select array_insert(array(1, 3, 4), -2, 2);
select array_insert(array(1, 2, 3), 3, "4");
select array_insert(cast(NULL as ARRAY<INT>), 1, 1);
select array_insert(array(1, 2, 3, NULL), cast(NULL as INT), 4);
select array_insert(array(1, 2, 3, NULL), 4, cast(NULL as INT));
select array_insert(array(2, 3, NULL, 4), 5, 5);
select array_insert(array(2, 3, NULL, 4), -5, 1);
select array_insert(array(1), 2, cast(2 as tinyint));

set spark.sql.legacy.negativeIndexInArrayInsert=true;
select array_insert(array(1, 3, 4), -2, 2);
select array_insert(array(2, 3, NULL, 4), -5, 1);
set spark.sql.legacy.negativeIndexInArrayInsert=false;

-- function array_compact
select array_compact(id) from values (1) as t(id);
select array_compact(array("1", null, "2", null));
select array_compact(array("a", "b", "c"));
select array_compact(array(1D, null, 2D, null));
select array_compact(array(array(1, 2, 3, null), null, array(4, null, 6)));
select array_compact(array(null));

-- function array_append
select array_append(array(1, 2, 3), 4);
select array_append(array('a', 'b', 'c'), 'd');
select array_append(array(1, 2, 3, NULL), NULL);
select array_append(array('a', 'b', 'c', NULL), NULL);
select array_append(CAST(null AS ARRAY<String>), 'a');
select array_append(CAST(null AS ARRAY<String>), CAST(null as String));
select array_append(array(), 1);
select array_append(CAST(array() AS ARRAY<String>), CAST(NULL AS String));
select array_append(array(CAST(NULL AS String)), CAST(NULL AS String));

-- function array_prepend
select array_prepend(array(1, 2, 3), 4);
select array_prepend(array('a', 'b', 'c'), 'd');
select array_prepend(array(1, 2, 3, NULL), NULL);
select array_prepend(array('a', 'b', 'c', NULL), NULL);
select array_prepend(CAST(null AS ARRAY<String>), 'a');
select array_prepend(CAST(null AS ARRAY<String>), CAST(null as String));
select array_prepend(array(), 1);
select array_prepend(CAST(array() AS ARRAY<String>), CAST(NULL AS String));
select array_prepend(array(CAST(NULL AS String)), CAST(NULL AS String));

-- SPARK-45599: Confirm 0.0, -0.0, and NaN are handled appropriately.
select array_union(array(0.0, -0.0, DOUBLE("NaN")), array(0.0, -0.0, DOUBLE("NaN")));
select array_distinct(array(0.0, -0.0, -0.0, DOUBLE("NaN"), DOUBLE("NaN")));
