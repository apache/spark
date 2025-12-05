-- Create some temporary test data.
create table t as
  select named_struct('u', named_struct('member0', member0, 'member1', member1)) as s
  from values (1, null), (null,  'a') tab(member0, member1);
declare avro_schema string;
set variable avro_schema =
  '{ "type": "record", "name": "struct", "fields": [{ "name": "u", "type": ["int","string"] }] }';

-- Exercise invalid SQL syntax when calling the 'from_avro' and 'to_avro' functions.
select from_avro(s, 42, map()) from t;
select from_avro(s, avro_schema, 42) from t;
select to_avro(s, 42) from t;

-- Avro is not loaded in this testing environment, so queries calling the 'from_avro' or 'to_avro'
-- SQL functions that otherwise pass analysis return appropriate "Avro not loaded" errors here.
select to_avro(s, avro_schema) as result from t;
select from_avro(result, avro_schema, map()).u from (select null as result);

-- Clean up.
drop temporary variable avro_schema;
drop table t;
