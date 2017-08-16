-- to_json
describe function to_json;
describe function extended to_json;
select to_json(named_struct('a', 1, 'b', 2));
select to_json(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
select to_json(array(named_struct('a', 1, 'b', 2)));
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
describe function json_tuple;
describe function extended json_tuple;
select json_tuple('{"a" : 1, "b" : 2}', cast(NULL AS STRING), 'b', cast(NULL AS STRING), 'a')
create temporary view jsonTable(jsonField, a, b) as select * from values '{"a": 1, "b": 2}', 'a', 'b';
SELECT json_tuple(jsonField, b, cast(NULL AS STRING), 'a') FROM jsonTable