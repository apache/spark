-- to_json
select to_json(named_struct('a', 1, 'b', 2));
select to_json(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
select to_json(array(named_struct('a', 1, 'b', 2)));
select to_json(map(named_struct('a', 1, 'b', 2), named_struct('a', 1, 'b', 2)));
select to_json(map('a', named_struct('a', 1, 'b', 2)));
select to_json(map('a', 1));
select to_json(array(map('a',1)));
select to_json(array(map('a',1), map('b',2)));
-- Check if errors handled
select to_json(named_struct('a', 1, 'b', 2), named_struct('mode', 'PERMISSIVE'));
select to_json(named_struct('a', 1, 'b', 2), map('mode', 1));
select to_json();

-- from_json
select from_json('{"a":1}', 'a INT');
select from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select from_json('{"a":1}', 1);
select from_json('{"a":1}', 'a InvalidType');
select from_json('{"a":1}', 'a INT', named_struct('mode', 'PERMISSIVE'));
select from_json('{"a":1}', 'a INT', map('mode', 1));
select from_json();
-- json_tuple
SELECT json_tuple('{"a" : 1, "b" : 2}', CAST(NULL AS STRING), 'b', CAST(NULL AS STRING), 'a');
CREATE TEMPORARY VIEW jsonTable(jsonField, a) AS SELECT * FROM VALUES ('{"a": 1, "b": 2}', 'a');
SELECT json_tuple(jsonField, 'b', CAST(NULL AS STRING), a) FROM jsonTable;
-- Clean up
DROP VIEW IF EXISTS jsonTable;

-- from_json - complex types
select from_json('{"a":1, "b":2}', 'map<string, int>');
select from_json('{"a":1, "b":"2"}', 'struct<a:int,b:string>');

-- infer schema of json literal
select schema_of_json('{"c1":0, "c2":[1]}');
select from_json('{"c1":[1, 2, 3]}', schema_of_json('{"c1":[0]}'));

-- from_json - array type
select from_json('[1, 2, 3]', 'array<int>');
select from_json('[1, "2", 3]', 'array<int>');
select from_json('[1, 2, null]', 'array<int>');

select from_json('[{"a": 1}, {"a":2}]', 'array<struct<a:int>>');
select from_json('{"a": 1}', 'array<struct<a:int>>');
select from_json('[null, {"a":2}]', 'array<struct<a:int>>');

select from_json('[{"a": 1}, {"b":2}]', 'array<map<string,int>>');
select from_json('[{"a": 1}, 2]', 'array<map<string,int>>');

-- to_json - array type
select to_json(array('1', '2', '3'));
select to_json(array(array(1, 2, 3), array(4)));

-- infer schema of json literal using options
select schema_of_json('{"c1":1}', map('primitivesAsString', 'true'));
select schema_of_json('{"c1":01, "c2":0.1}', map('allowNumericLeadingZeros', 'true', 'prefersDecimal', 'true'));
select schema_of_json(null);
CREATE TEMPORARY VIEW jsonTable(jsonField, a) AS SELECT * FROM VALUES ('{"a": 1, "b": 2}', 'a');
SELECT schema_of_json(jsonField) FROM jsonTable;
-- Clean up
DROP VIEW IF EXISTS jsonTable;
