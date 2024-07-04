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
CREATE TEMPORARY VIEW xmlTable(xmlField, a) AS SELECT * FROM VALUES ('<p><a>1</a><b>"2"</b></p>', 'a');
SELECT schema_of_xml(xmlField) FROM xmlTable;

-- Clean up
DROP VIEW IF EXISTS xmlTable;
