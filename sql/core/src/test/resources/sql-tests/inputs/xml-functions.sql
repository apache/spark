-- to_xml
select to_xml(named_struct('a', 1, 'b', 2), map('indent', ''));
select to_xml(named_struct('time', to_timestamp('2015-08-26', 'yyyy-MM-dd')), map('timestampFormat', 'dd/MM/yyyy', 'indent', ''));
-- Check if errors handled
select to_xml(array(named_struct('a', 1, 'b', 2)));
select to_xml(map('a', 1));
select to_xml(named_struct('a', 1, 'b', 2), named_struct('mode', 'PERMISSIVE'));
select to_xml(named_struct('a', 1, 'b', 2), map('mode', 1));
select to_xml();

-- from_xml
select from_xml('<p><a>1</a></p>', 'a INT');
select from_xml('<p><time>26/08/2015</time></p>', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
-- Check if errors handled
select from_xml('<p><a>1</a></p>', 1);
select from_xml('<p><a>1</a></p>', 'a InvalidType');
select from_xml('<p><a>1</a></p>', 'a INT', named_struct('mode', 'PERMISSIVE'));
select from_xml('<p><a>1</a></p>', 'a INT', map('mode', 1));
select from_xml();

-- Clean up
DROP VIEW IF EXISTS xmlTable;

-- from_xml - complex types
select from_xml('<p><a>1</a></p>', 'struct<a:array<int>>');
select from_xml('<p><a>1</a><b>"2"</b></p>', 'struct<a:int,b:string>');

-- infer schema of xml literal
select schema_of_xml('<p><a>1</a><b>"2"</b></p>');
select from_xml('<p><a>1</a><a>2</a><a>3</a></p>', schema_of_xml('<p><a>1</a><a>2</a></p>'));

-- from_xml - array type
select from_xml('<p><a>1</a><a>2</a></p>', 'struct<a:array<int>>');
select from_xml('<p><a>1</a><a>"2"</a></p>', 'struct<a:array<int>>');
select from_xml('<p><a>1</a><a></a></p>', 'struct<a:array<int>>');

select from_xml('<p><a attr="1"><b>2</b></a></p>', 'struct<a:map<string,int>>');

-- from_xml - datetime type
select from_xml('<p><d>2012-12-15</d><t>2012-12-15 15:15:15</t></p>', 'd date, t timestamp');
select from_xml(
  '<p><d>12/15 2012</d><t>12/15 2012 15:15:15</t>}</p>',
  'd date, t timestamp',
  map('dateFormat', 'MM/dd yyyy', 'timestampFormat', 'MM/dd yyyy HH:mm:ss'));
select from_xml(
  '<p><d>02-29</d></p>',
  'd date',
  map('dateFormat', 'MM-dd'));
select from_xml(
  '<p><t>02-29</t></p>',
  't timestamp',
  map('timestampFormat', 'MM-dd'));

-- infer schema of xml literal with options
select schema_of_xml(null);
select schema_of_xml(42);
CREATE TEMPORARY VIEW xmlTable(xmlField, a) AS SELECT * FROM VALUES ('<p><a>1</a><b>"2"</b></p>', 'a');
SELECT schema_of_xml(xmlField) FROM xmlTable;

-- Clean up
DROP VIEW IF EXISTS xmlTable;

-- TIME type tests
-- from_xml with TIME type
select from_xml('<record><time>14:30:45</time></record>', 'time TIME(0)', map('rowTag', 'record'));
select from_xml('<record><time>14:30:45.123</time></record>', 'time TIME(3)', map('rowTag', 'record'));
select from_xml('<record><time>14:30:45.123456</time></record>', 'time TIME(6)', map('rowTag', 'record'));
select from_xml('<record><time>14-30-45.123456</time></record>', 'time TIME(6)', map('rowTag', 'record', 'timeFormat', 'HH-mm-ss.SSSSSS'));
select from_xml('<record><t1>09:00:00</t1><t2>17:30:00</t2></record>', 't1 TIME, t2 TIME', map('rowTag', 'record'));

-- Invalid input
select from_xml('<record><time>25:00:00</time></record>', 'time TIME', map('rowTag', 'record'));
select from_xml('<record><time>invalid</time></record>', 'time TIME', map('rowTag', 'record'));
select from_xml('<record></record>', 'time TIME', map('rowTag', 'record'));

-- to_xml with TIME type
select to_xml(named_struct('time', TIME'14:30:45'), map('rowTag', 'record', 'indent', ''));
select to_xml(named_struct('time', TIME'14:30:45.123456'), map('rowTag', 'record', 'indent', ''));
select to_xml(named_struct('time', TIME'14:30:45.123456'), map('rowTag', 'record', 'timeFormat', 'HH-mm-ss.SSSSSS', 'indent', ''));

-- TIME type roundtrip tests
select from_xml(to_xml(named_struct('time', TIME'14:30:45'), map('rowTag', 'record')), 'time TIME(0)', map('rowTag', 'record'));
select from_xml(to_xml(named_struct('time', TIME'14:30:45.1'), map('rowTag', 'record')), 'time TIME(1)', map('rowTag', 'record'));
select from_xml(to_xml(named_struct('time', TIME'14:30:45.12'), map('rowTag', 'record')), 'time TIME(2)', map('rowTag', 'record'));
select from_xml(to_xml(named_struct('time', TIME'14:30:45.123'), map('rowTag', 'record')), 'time TIME(3)', map('rowTag', 'record'));
select from_xml(to_xml(named_struct('time', TIME'14:30:45.1234'), map('rowTag', 'record')), 'time TIME(4)', map('rowTag', 'record'));
select from_xml(to_xml(named_struct('time', TIME'14:30:45.12345'), map('rowTag', 'record')), 'time TIME(5)', map('rowTag', 'record'));
select from_xml(to_xml(named_struct('time', TIME'14:30:45.123456'), map('rowTag', 'record')), 'time TIME(6)', map('rowTag', 'record'));

-- Reverse roundtrip
select to_xml(from_xml('<record><time>14:30:45</time></record>', 'time TIME(0)', map('rowTag', 'record')), map('rowTag', 'record', 'indent', ''));
select to_xml(from_xml('<record><time>14:30:45.123</time></record>', 'time TIME(3)', map('rowTag', 'record')), map('rowTag', 'record', 'indent', ''));
select to_xml(from_xml('<record><time>14:30:45.123456</time></record>', 'time TIME(6)', map('rowTag', 'record')), map('rowTag', 'record', 'indent', ''));

-- TIME type schema inference
-- Schema inference: TIME type is never auto-inferred (requires explicit schema).
-- Time-like strings are inferred as TIMESTAMP by XML's existing inference logic.
select schema_of_xml('<record><time>14:30:45</time></record>', map('rowTag', 'record'));
select schema_of_xml('<record><time>14:30:45.123456</time></record>', map('rowTag', 'record'));

-- LIMIT test
select from_xml('<record><time>14:30:45</time></record>', 'time TIME', map('rowTag', 'record')) LIMIT 1;
