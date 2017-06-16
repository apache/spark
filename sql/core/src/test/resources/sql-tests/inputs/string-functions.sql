-- Argument number exception
select concat_ws();
select format_string();

-- A pipe operator for string concatenation
select 'a' || 'b' || 'c';

-- Check if catalyst combine nested `Concat`s
EXPLAIN EXTENDED SELECT (col1 || col2 || col3 || col4) col
FROM (SELECT id col1, id col2, id col3, id col4 FROM range(10)) t;

-- replace function
select replace('abc', 'b', '123');
select replace('abc', 'b');

-- uuid
select length(uuid()), (uuid() <> uuid());

-- position
select position('bar' in 'foobarbar'), position(null, 'foobarbar'), position('aaads', null);
