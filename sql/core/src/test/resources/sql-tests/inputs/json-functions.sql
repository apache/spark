-- to_json
describe function to_json;
describe function extended to_json;
select to_json(named_struct('a', 1, 'b', 2));
select to_json(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select to_json(named_struct('a', 1, 'b', 2), named_struct('mode', 'PERMISSIVE'));
select to_json();

-- from_json
describe function from_json;
describe function extended from_json;
select from_json('{"a":1}', '{"type":"struct", "fields":[{"name":"a", "type":"integer", "nullable":true}]}');
select from_json('{"time":"26/08/2015"}', '{"type":"struct", "fields":[{"name":"time", "type":"timestamp", "nullable":true}]}', map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select from_json('{"a":1}', 1);
select from_json('{"a":1}', '{"a": 1}');
select from_json('{"a":1}', '{"type":"struct", "fields":[{"name":"a", "type":"integer", "nullable":true}]}', named_struct('mode', 'PERMISSIVE'));
select from_json();
