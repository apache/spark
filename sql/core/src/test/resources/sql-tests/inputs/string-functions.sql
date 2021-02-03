-- Argument number exception
select concat_ws();
select format_string();

-- A pipe operator for string concatenation
select 'a' || 'b' || 'c';

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

-- split function
SELECT split('aa1cc2ee3', '[1-9]+');
SELECT split('aa1cc2ee3', '[1-9]+', 2);

-- substring function
SELECT substr('Spark SQL', 5);
SELECT substr('Spark SQL', -3);
SELECT substr('Spark SQL', 5, 1);
SELECT substr('Spark SQL' from 5);
SELECT substr('Spark SQL' from -3);
SELECT substr('Spark SQL' from 5 for 1);
SELECT substring('Spark SQL', 5);
SELECT substring('Spark SQL', -3);
SELECT substring('Spark SQL', 5, 1);
SELECT substring('Spark SQL' from 5);
SELECT substring('Spark SQL' from -3);
SELECT substring('Spark SQL' from 5 for 1);

-- trim
SELECT trim(" xyz "), ltrim(" xyz "), rtrim(" xyz ");
SELECT trim(BOTH 'xyz' FROM 'yxTomxx'), trim('xyz' FROM 'yxTomxx');
SELECT trim(BOTH 'x' FROM 'xxxbarxxx'), trim('x' FROM 'xxxbarxxx');
SELECT trim(LEADING 'xyz' FROM 'zzzytest');
SELECT trim(LEADING 'xyz' FROM 'zzzytestxyz');
SELECT trim(LEADING 'xy' FROM 'xyxXxyLAST WORD');
SELECT trim(TRAILING 'xyz' FROM 'testxxzx');
SELECT trim(TRAILING 'xyz' FROM 'xyztestxxzx');
SELECT trim(TRAILING 'xy' FROM 'TURNERyxXxy');

-- Check lpad/rpad with invalid length parameter
SELECT lpad('hi', 'invalid_length');
SELECT rpad('hi', 'invalid_length');

-- decode
select decode();
select decode(encode('abc', 'utf-8'));
select decode(encode('abc', 'utf-8'), 'utf-8');
select decode(1, 1, 'Southlake');
select decode(2, 1, 'Southlake');
select decode(2, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle');

-- to_number
select to_number('454', '999');
select to_number('454', '000');
select to_number('054', '999');
select to_number('054', '000');
select to_number('454.2', '999.9');
select to_number('454.2', '000.0');
select to_number('454.2', '999D9');
select to_number('454.2', '000D0');
select to_number('12,454', '99,999');
select to_number('12,454', '00,000');
select to_number('12,454', '99G999');
select to_number('12,454', '00G000');
select to_number('12,454,367', '99,999,999');
select to_number('12,454,367', '00,000,000');
select to_number('12,454,367', '99G999G999');
select to_number('12,454,367', '00G000G000');
select to_number('$78.12', '$99.99');
select to_number('$78.12', '$00.00');
select to_number('78.12$', '99.99$');
select to_number('78.12$', '00.00$');
select to_number('-454', 'S999');
select to_number('-454', 'S000');
select to_number('454-', '999S');
select to_number('454-', '000S');