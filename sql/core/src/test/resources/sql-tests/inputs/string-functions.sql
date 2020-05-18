-- Argument number exception
select concat_ws();
select format_string();

-- A pipe operator for string concatenation
select 'a' || 'b' || 'c';

-- Check if catalyst combine nested `Concat`s
EXPLAIN EXTENDED SELECT (col1 || col2 || col3 || col4) col
FROM (SELECT id col1, id col2, id col3, id col4 FROM range(10));

-- replace function
select replace('abc', 'b', '123');
select replace('abc', 'b');

-- uuid
select length(uuid()), (uuid() <> uuid());

-- position
select position('bar' in 'foobarbar'), position(null, 'foobarbar'), position('aaads', null);

-- left && right
select left("abcd", 2), left("abcd", 5), left("abcd", '2'), left("abcd", null);
select left(null, -2), left("abcd", -2), left("abcd", 0), left("abcd", 'a');
select right("abcd", 2), right("abcd", 5), right("abcd", '2'), right("abcd", null);
select right(null, -2), right("abcd", -2), right("abcd", 0), right("abcd", 'a');

-- turn off concatBinaryAsString
set spark.sql.function.concatBinaryAsString=false;

-- Check if catalyst combine nested `Concat`s if concatBinaryAsString=false
EXPLAIN SELECT ((col1 || col2) || (col3 || col4)) col
FROM (
  SELECT
    string(id) col1,
    string(id + 1) col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

EXPLAIN SELECT (col1 || (col3 || col4)) col
FROM (
  SELECT
    string(id) col1,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

-- trim
SELECT trim(" xyz "), ltrim(" xyz "), rtrim(" xyz ");
SELECT trim('xyz', 'yxTomxx'), ltrim('xyz', 'yxTomxx'), rtrim('xyz', 'yxTomxx');