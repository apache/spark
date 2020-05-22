CREATE TEMPORARY VIEW struct_level_1 AS VALUES
  (CAST(STRUCT(1, NULL, 3) AS struct<a:int,b:int,c:int>))
  AS T(a);

CREATE TEMPORARY VIEW null_struct_level_1 AS VALUES
  (CAST(NULL AS struct<a:int,b:int,c:int>))
  AS T(a);

CREATE TEMPORARY VIEW struct_level_2 AS VALUES
  (CAST(STRUCT(STRUCT(1, NULL, 3)) AS struct<a:struct<a:int,b:int,c:int>>))
  AS T(a);

CREATE TEMPORARY VIEW struct_level_3 AS VALUES
  (CAST(STRUCT(STRUCT(STRUCT(1, NULL, 3))) AS struct<a:struct<a:struct<a:int,b:int,c:int>>>))
  AS T(a);

CREATE TEMPORARY VIEW mixed_case_struct_level_1 AS VALUES
  (CAST(STRUCT(1, 1) AS struct<a:int,B:int>))
  AS T(a);

CREATE TEMPORARY VIEW mixed_case_struct_level_2 AS VALUES
  (CAST(STRUCT(STRUCT(1, 1), STRUCT(1, 1)) AS struct<a:struct<a:int,b:int>,B:struct<a:int,b:int>>))
  AS T(a);

CREATE TEMPORARY VIEW struct_with_dots_in_name_level_2 AS VALUES
  (CAST(STRUCT(STRUCT(1, NULL, 3)) AS struct<`a.b`:struct<`c.d`:int,`e.f`:int,`g.h`:int>>))
  AS T(a);

-- Should throw an exception if no arguments are provided
SELECT WITH_FIELD() AS a FROM struct_level_1;

-- Should throw an exception if fieldName and value are missing
SELECT WITH_FIELD(a) AS a FROM struct_level_1;

-- Should throw an exception if value is missing
SELECT WITH_FIELD(a, 'd') AS a FROM struct_level_1;

-- Should throw an exception if first argument is not a struct
SELECT WITH_FIELD(1, 'd', 4) AS a FROM struct_level_1;

-- Should throw an exception if fieldName is null
SELECT WITH_FIELD(a, NULL, 2) AS a FROM struct_level_1;

-- Should throw an exception if fieldName is not a string
SELECT WITH_FIELD(a, 1, 2) AS a FROM struct_level_1;

-- Should throw an exception if an intermediate field is not a struct
SELECT WITH_FIELD(a, 'b.a', 2) AS a FROM struct_level_1;

-- Should add field to struct
SELECT WITH_FIELD(a, 'd', 4) AS a FROM struct_level_1;

-- Should add field to null struct
SELECT WITH_FIELD(a, 'd', 4) a FROM null_struct_level_1;

-- Should add field with null value to struct
SELECT WITH_FIELD(a, 'd', NULL) AS a FROM struct_level_1;

-- Should add multiple fields to struct
SELECT WITH_FIELD(WITH_FIELD(a, 'd', 4), 'e', 5) AS a FROM struct_level_1;

-- Should add field to nested struct
SELECT WITH_FIELD(a, 'a', WITH_FIELD(a.a, 'd', 4)) AS a FROM struct_level_2;
SELECT WITH_FIELD(a, 'a.d', 4) AS a FROM struct_level_2;

-- Should add field to deeply nested struct
SELECT WITH_FIELD(a, 'a.a.d', 4) AS a FROM struct_level_3;

-- Should add intermediate structs and nested field
SELECT WITH_FIELD(a, 'x.y', 4) AS a FROM struct_level_2;

-- Should add intermediate structs and deeply nested field
SELECT WITH_FIELD(a, 'x.y.z', 4) AS a FROM struct_level_3;

-- Should replace field in struct
SELECT WITH_FIELD(a, 'b', 2) AS a FROM struct_level_1;

-- Should replace field in null struct
SELECT WITH_FIELD(a, 'b', 2) AS a FROM null_struct_level_1;

-- Should replace field with null value in struct
SELECT WITH_FIELD(a, 'c', NULL) AS a FROM struct_level_1;

-- Should replace multiple fields in struct
SELECT WITH_FIELD(WITH_FIELD(a, 'a', 10), 'b', 20) AS a FROM struct_level_1;

-- Should replace field in nested struct
SELECT WITH_FIELD(a, 'a', WITH_FIELD(a.a, 'b', 2)) AS a FROM struct_level_2;
SELECT WITH_FIELD(a, 'a.b', 2) AS a FROM struct_level_2;

-- Should replace field in deeply nested struct
SELECT WITH_FIELD(a, 'a.a.b', 2) AS a FROM struct_level_3;

-- Should replace all fields with given name in struct
SELECT WITH_FIELD(a, 'b', 100) AS a FROM (VALUES (NAMED_STRUCT('a', 1, 'b', 2, 'b', 3)) AS nested_struct(a));

-- Should replace fields in struct in given order
SELECT WITH_FIELD(WITH_FIELD(a, 'b', 2), 'b', 20) AS a FROM struct_level_1;

-- should add field and then replace same field in struct
SELECT WITH_FIELD(WITH_FIELD(a, 'd', 4), 'd', 5) AS a FROM struct_level_1;

-- Should handle fields with dots in their name if correctly quoted
SELECT WITH_FIELD(a, '`a.b`.`e.f`', 2) AS a FROM struct_with_dots_in_name_level_2;
SELECT WITH_FIELD(a, 'a.b.e.f', 2) AS a FROM struct_with_dots_in_name_level_2;

-- Should replace field in struct even if casing is different
set spark.sql.caseSensitive=false;
SELECT WITH_FIELD(a, 'A', 2) AS a FROM mixed_case_struct_level_1;
SELECT WITH_FIELD(a, 'b', 2) AS a FROM mixed_case_struct_level_1;

-- Should add field in struct because casing is different
set spark.sql.caseSensitive=true;
SELECT WITH_FIELD(a, 'A', 2) AS a FROM mixed_case_struct_level_1;
SELECT WITH_FIELD(a, 'b', 2) AS a FROM mixed_case_struct_level_1;

-- Should replace nested field in struct even if casing is different
set spark.sql.caseSensitive=false;
SELECT WITH_FIELD(a, 'A.a', 2) AS a FROM mixed_case_struct_level_2;
SELECT WITH_FIELD(a, 'b.a', 2) AS a FROM mixed_case_struct_level_2;

-- Should add nested field to struct because casing is different
set spark.sql.caseSensitive=true;
SELECT WITH_FIELD(a, 'A.a', 2) AS a FROM mixed_case_struct_level_2;
SELECT WITH_FIELD(a, 'b.a', 2) AS a FROM mixed_case_struct_level_2;