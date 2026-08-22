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
-- json_tuple exists no foldable null field
SELECT json_tuple('{"a":"1"}', if(c1 < 1, null, 'a')) FROM ( SELECT rand() AS c1 );
SELECT json_tuple('{"a":"1"}', if(c1 < 1, null, 'a'), if(c2 < 1, null, 'a')) FROM ( SELECT 0 AS c1, rand() AS c2 );

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

-- from_json - datetime type
select from_json('{"d": "2012-12-15", "t": "2012-12-15 15:15:15"}', 'd date, t timestamp');
select from_json(
  '{"d": "12/15 2012", "t": "12/15 2012 15:15:15"}',
  'd date, t timestamp',
  map('dateFormat', 'MM/dd yyyy', 'timestampFormat', 'MM/dd yyyy HH:mm:ss'));
select from_json(
  '{"d": "02-29"}',
  'd date',
  map('dateFormat', 'MM-dd'));
select from_json(
  '{"t": "02-29"}',
  't timestamp',
  map('timestampFormat', 'MM-dd'));

-- to_json - array type
select to_json(array('1', '2', '3'));
select to_json(array(array(1, 2, 3), array(4)));

-- infer schema of json literal using options
select schema_of_json('{"c1":1}', map('primitivesAsString', 'true'));
select schema_of_json('{"c1":01, "c2":0.1}', map('allowNumericLeadingZeros', 'true', 'prefersDecimal', 'true'));
select schema_of_json(null);
select schema_of_json(42);
CREATE TEMPORARY VIEW jsonTable(jsonField, a) AS SELECT * FROM VALUES ('{"a": 1, "b": 2}', 'a');
SELECT schema_of_json(jsonField) FROM jsonTable;

-- json_array_length
select json_array_length(null);
select json_array_length(2);
select json_array_length();
select json_array_length('');
select json_array_length('[]');
select json_array_length('[1,2,3]');
select json_array_length('[[1,2],[5,6,7]]');
select json_array_length('[{"a":123},{"b":"hello"}]');
select json_array_length('[1,2,3,[33,44],{"key":[2,3,4]}]');
select json_array_length('{"key":"not a json array"}');
select json_array_length('[1,2,3,4,5');

-- json_object_keys
select json_object_keys();
select json_object_keys(null);
select json_object_keys(200);
select json_object_keys('');
select json_object_keys('{}');
select json_object_keys('{"key": 1}');
select json_object_keys('{"key": "value", "key2": 2}');
select json_object_keys('{"arrayKey": [1, 2, 3]}');
select json_object_keys('{"key":[1,2,3,{"key":"value"},[1,2,3]]}');
select json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}');
select json_object_keys('{"k1": [1, 2, {"key": 5}], "k2": {"key2": [1, 2]}}');
select json_object_keys('{[1,2]}');
select json_object_keys('{"key": 45, "random_string"}');
select json_object_keys('[1, 2, 3]');

-- json_typeof
select json_typeof();
select json_typeof(null);
select json_typeof(200);
select json_typeof('');
select json_typeof('{}');
select json_typeof('{"key": 1, "arr": [1, 2]}');
select json_typeof('[]');
select json_typeof('[1, 2, 3]');
select json_typeof('"hello"');
select json_typeof('123');
select json_typeof('1.5');
select json_typeof('-123');
select json_typeof('-1.5');
select json_typeof('true');
select json_typeof('false');
select json_typeof('null');
select json_typeof('bad');
select json_typeof('{"key": 45, "random_string"}');
select json_typeof('123 true');

-- Clean up
DROP VIEW IF EXISTS jsonTable;

-- TIME type tests
-- from_json with TIME type
select from_json('{"time": "14:30:45"}', 'time TIME(0)');
select from_json('{"time": "14:30:45.123"}', 'time TIME(3)');
select from_json('{"time": "14:30:45.123456"}', 'time TIME(6)');
select from_json('{"time": "14-30-45.123456"}', 'time TIME(6)', map('timeFormat', 'HH-mm-ss.SSSSSS'));
select from_json('{"t1": "09:00:00", "t2": "17:30:00"}', 't1 TIME, t2 TIME');
select from_json('{"time": "25:00:00"}', 'time TIME');
select from_json('{"time": "invalid"}', 'time TIME');
select from_json('{"time": null}', 'time TIME');

-- to_json with TIME type
select to_json(named_struct('time', TIME'14:30:45'));
select to_json(named_struct('time', TIME'14:30:45.123456'));
select to_json(named_struct('time', TIME'14:30:45.123456'), map('timeFormat', 'HH-mm-ss.SSSSSS'));
select to_json(array(TIME'09:00:00', TIME'17:45:30'));

-- TIME type roundtrip tests
select from_json(to_json(named_struct('time', TIME'14:30:45')), 'time TIME(0)');
select from_json(to_json(named_struct('time', TIME'14:30:45.1')), 'time TIME(1)');
select from_json(to_json(named_struct('time', TIME'14:30:45.12')), 'time TIME(2)');
select from_json(to_json(named_struct('time', TIME'14:30:45.123')), 'time TIME(3)');
select from_json(to_json(named_struct('time', TIME'14:30:45.1234')), 'time TIME(4)');
select from_json(to_json(named_struct('time', TIME'14:30:45.12345')), 'time TIME(5)');
select from_json(to_json(named_struct('time', TIME'14:30:45.123456')), 'time TIME(6)');
select from_json(to_json(named_struct('time', TIME'00:00:00')), 'time TIME(0)');
select from_json(to_json(named_struct('time', TIME'23:59:59.999999')), 'time TIME(6)');
select to_json(from_json('{"time":"14:30:45"}', 'time TIME(0)'));
select to_json(from_json('{"time":"14:30:45.1"}', 'time TIME(1)'));
select to_json(from_json('{"time":"14:30:45.12"}', 'time TIME(2)'));
select to_json(from_json('{"time":"14:30:45.123"}', 'time TIME(3)'));
select to_json(from_json('{"time":"14:30:45.1234"}', 'time TIME(4)'));
select to_json(from_json('{"time":"14:30:45.12345"}', 'time TIME(5)'));
select to_json(from_json('{"time":"14:30:45.123456"}', 'time TIME(6)'));
select to_json(from_json('{"time":"00:00:00"}', 'time TIME(0)'));
select to_json(from_json('{"time":"23:59:59.999999"}', 'time TIME(6)'));

-- TIME type schema inference and other tests
select schema_of_json('{"time": "14:30:45"}');
select schema_of_json('{"time": "14:30:45.123456"}');
select from_json('{"time": "14:30:45"}', 'time TIME') LIMIT 1;

-- JSON_VALUE: extract a scalar value (ANSI SQL:2016)
select json_value('{"id":7,"name":"Ada","tags":["x","y"],"addr":{"city":"NYC"},"score":null}', '$.name');
select json_value('{"id":7,"name":"Ada","tags":["x","y"],"addr":{"city":"NYC"},"score":null}', '$.id' RETURNING INT);
select json_value('{"id":7,"name":"Ada"}', '$.id' RETURNING INT) + 1;
-- present but JSON null -> SQL NULL
select json_value('{"score":null}', '$.score');
-- non-scalar (object / array) -> NULL ON ERROR (default)
select json_value('{"addr":{"city":"NYC"}}', '$.addr');
select json_value('{"tags":["x","y"]}', '$.tags');
-- missing path -> NULL ON EMPTY (default)
select json_value('{"id":7}', '$.missing');
-- NULL input propagates to NULL
select json_value(cast(null as string), '$.a');
-- ON EMPTY behaviors
select json_value('{"id":7}', '$.missing' DEFAULT '?' ON EMPTY);
select json_value('{"id":7}', '$.missing' RETURNING INT DEFAULT 42 ON EMPTY);
select json_value('{"id":7}', '$.missing' ERROR ON EMPTY);
-- ON ERROR behaviors
select json_value('{"addr":{"city":"NYC"}}', '$.addr' DEFAULT 'n/a' ON ERROR);
select json_value('not json', '$.a' DEFAULT 'bad' ON ERROR);
select json_value('not json', '$.a' ERROR ON ERROR);
-- failed cast -> ON ERROR
select json_value('{"name":"Ada"}', '$.name' RETURNING INT);
select json_value('{"name":"Ada"}', '$.name' RETURNING INT ERROR ON ERROR);
select json_value('{"name":"Ada"}', '$.name' RETURNING INT DEFAULT -1 ON ERROR);
-- combined ON EMPTY + ON ERROR
select json_value('{"a":"x"}', '$.b' DEFAULT 'e' ON EMPTY DEFAULT 'r' ON ERROR);
-- RETURNING types
select json_value('{"v":"3.14"}', '$.v' RETURNING DOUBLE);
select json_value('{"v":"true"}', '$.v' RETURNING BOOLEAN);
select json_value('{"v":"2020-01-02"}', '$.v' RETURNING DATE);
-- invalid: wildcard path
select json_value('{"a":[1,2]}', '$.a[*]');
-- invalid: non-scalar RETURNING type
select json_value('{"a":1}', '$.a' RETURNING STRUCT<x:INT>);
-- invalid: a DEFAULT that cannot cast to the RETURNING type
select json_value('{}', '$.x' RETURNING INT DEFAULT array(1) ON EMPTY);

-- JSON_EXISTS: test path presence (ANSI SQL:2016)
select json_exists('{"id":7,"addr":{"city":"NYC"},"score":null,"tags":["x","y"]}', '$.addr.city');
-- present but JSON null -> true
select json_exists('{"score":null}', '$.score');
-- absent -> false
select json_exists('{"addr":{"city":"NYC"}}', '$.addr.zip');
-- matches an object / array -> true
select json_exists('{"addr":{"city":"NYC"}}', '$.addr');
select json_exists('{"tags":["x","y"]}', '$.tags[0]');
-- NULL input -> NULL (unknown)
select json_exists(cast(null as string), '$.a');
-- malformed input -> FALSE ON ERROR (default)
select json_exists('not json', '$.a');
-- ON ERROR behaviors
select json_exists('not json', '$.a' TRUE ON ERROR);
select json_exists('not json', '$.a' FALSE ON ERROR);
select json_exists('not json', '$.a' UNKNOWN ON ERROR);
select json_exists('not json', '$.a' ERROR ON ERROR);
-- lax wildcard [*]: true iff the array has elements
select json_exists('{"a":[1,2]}', '$.a[*]');
select json_exists('{"a":[]}', '$.a[*]');
-- lax auto-wrap: [*] over a non-array treats it as a single-element array
select json_exists('{"a":5}', '$.a[*]');
-- embedded wildcard: any element has the field
select json_exists('{"a":[{"b":1},{"c":2}]}', '$.a[*].b');
-- out-of-range index -> false
select json_exists('{"a":[1,2]}', '$.a[5]');
-- lax auto-unwrap: a member step over an array applies to each element
select json_exists('{"a":[{"b":1},{"b":2}]}', '$.a.b');
-- member wildcard .* matches any member
select json_exists('{"addr":{"city":"NYC"}}', '$.*');
-- invalid: an unparseable path is rejected at analysis
select json_exists('{"a":1}', '$[');

-- JSON_QUERY: extract an object or array as JSON text (ANSI SQL:2016)
select json_query('{"id":7,"name":"Ada","tags":["x","y"],"addr":{"city":"NYC"},"score":null}', '$.addr');
select json_query('{"id":7,"name":"Ada","tags":["x","y"],"addr":{"city":"NYC"},"score":null}', '$.tags');
-- a scalar result is emitted as JSON text (not an error) under the default WITHOUT ARRAY WRAPPER
select json_query('{"id":7}', '$.id');
select json_query('{"name":"Ada"}', '$.name');
-- present but JSON null -> the JSON text null
select json_query('{"score":null}', '$.score');
-- missing path -> NULL ON EMPTY (default)
select json_query('{"id":7}', '$.missing');
-- NULL input propagates to NULL
select json_query(cast(null as string), '$.a');
-- ARRAY WRAPPER
select json_query('{"tags":["x","y"]}', '$.tags[0]' WITH ARRAY WRAPPER);
select json_query('{"tags":["x","y"]}', '$.tags' WITH UNCONDITIONAL ARRAY WRAPPER);
select json_query('{"id":7}', '$.id' WITH ARRAY WRAPPER);
-- CONDITIONAL wraps only a scalar; an object/array is left as is
select json_query('{"id":7}', '$.id' WITH CONDITIONAL ARRAY WRAPPER);
select json_query('{"addr":{"city":"NYC"}}', '$.addr' WITH CONDITIONAL ARRAY WRAPPER);
-- OMIT QUOTES strips the quotes from a scalar string result
select json_query('{"name":"Ada"}', '$.name' OMIT QUOTES);
select json_query('{"name":"Ada"}', '$.name' KEEP QUOTES);
-- ON EMPTY behaviors
select json_query('{"id":7}', '$.missing' EMPTY ARRAY ON EMPTY);
select json_query('{"id":7}', '$.missing' EMPTY OBJECT ON EMPTY);
select json_query('{"id":7}', '$.missing' ERROR ON EMPTY);
-- ON ERROR behaviors (malformed input)
select json_query('not json', '$.a');
select json_query('not json', '$.a' EMPTY ARRAY ON ERROR);
select json_query('not json', '$.a' EMPTY OBJECT ON ERROR);
select json_query('not json', '$.a' ERROR ON ERROR);
-- RETURNING STRING is allowed (the result is JSON text)
select json_query('{"addr":{"city":"NYC"}}', '$.addr' RETURNING STRING);
-- invalid: wildcard path
select json_query('{"a":[1,2]}', '$.a[*]');
-- invalid: non-string RETURNING type
select json_query('{"a":1}', '$.a' RETURNING INT);
-- invalid: OMIT QUOTES combined with an array wrapper
select json_query('{"name":"Ada"}', '$.name' WITH ARRAY WRAPPER OMIT QUOTES);
