-- to_json
describe function to_json;
describe function extended to_json;
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
describe function from_json;
describe function extended from_json;
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
