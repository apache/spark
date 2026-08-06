-- Tests that set operations (and the DISTINCT/de-duplication they rely on) are
-- rejected when a column has MAP or VARIANT type, since those types are not
-- orderable. See UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE and
-- UNSUPPORTED_FEATURE.SET_OPERATION_ON_VARIANT_TYPE.

create temporary view map_view as select map(1, 2) as m, id from range(2);

create temporary view variant_view as select parse_json('1') as v, id from range(2);


-- MAP type columns in set operations
SELECT * FROM map_view INTERSECT SELECT * FROM map_view;

SELECT * FROM map_view EXCEPT SELECT * FROM map_view;

SELECT * FROM map_view UNION SELECT * FROM map_view;

SELECT DISTINCT m FROM map_view;


-- VARIANT type columns in set operations
SELECT * FROM variant_view INTERSECT SELECT * FROM variant_view;

SELECT * FROM variant_view EXCEPT SELECT * FROM variant_view;

SELECT * FROM variant_view UNION SELECT * FROM variant_view;

SELECT DISTINCT v FROM variant_view;


-- VARIANT nested inside complex types is also rejected
SELECT DISTINCT struct(v) FROM variant_view;

SELECT DISTINCT array(v) FROM variant_view;

SELECT DISTINCT map('m', v) FROM variant_view;
