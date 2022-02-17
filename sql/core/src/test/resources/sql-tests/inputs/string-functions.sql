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
select left(null, -2);
select left("abcd", -2), left("abcd", 0), left("abcd", 'a');
select right("abcd", 2), right("abcd", 5), right("abcd", '2'), right("abcd", null);
select right(null, -2);
select right("abcd", -2), right("abcd", 0), right("abcd", 'a');

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

-- btrim
SELECT btrim('xyxtrimyyx', 'xy');
SELECT btrim(encode(" xyz ", 'utf-8'));
SELECT btrim(encode('yxTomxx', 'utf-8'), encode('xyz', 'utf-8'));
SELECT btrim(encode('xxxbarxxx', 'utf-8'), encode('x', 'utf-8'));

-- Check lpad/rpad with invalid length parameter
SELECT lpad('hi', 'invalid_length');
SELECT rpad('hi', 'invalid_length');

-- lpad for BINARY inputs
SELECT hex(lpad(unhex(''), 5));
SELECT hex(lpad(unhex('aabb'), 5));
SELECT hex(lpad(unhex('aabbcc'), 2));
SELECT hex(lpad(unhex(''), 5, unhex('1f')));
SELECT hex(lpad(unhex('aa'), 5, unhex('1f')));
SELECT hex(lpad(unhex('aa'), 6, unhex('1f')));
SELECT hex(lpad(unhex(''), 5, unhex('1f2e')));
SELECT hex(lpad(unhex('aa'), 5, unhex('1f2e')));
SELECT hex(lpad(unhex('aa'), 6, unhex('1f2e')));
SELECT hex(lpad(unhex(''), 6, unhex('')));
SELECT hex(lpad(unhex('aabbcc'), 6, unhex('')));
SELECT hex(lpad(unhex('aabbcc'), 2, unhex('ff')));

-- rpad for BINARY inputs
SELECT hex(rpad(unhex(''), 5));
SELECT hex(rpad(unhex('aabb'), 5));
SELECT hex(rpad(unhex('aabbcc'), 2));
SELECT hex(rpad(unhex(''), 5, unhex('1f')));
SELECT hex(rpad(unhex('aa'), 5, unhex('1f')));
SELECT hex(rpad(unhex('aa'), 6, unhex('1f')));
SELECT hex(rpad(unhex(''), 5, unhex('1f2e')));
SELECT hex(rpad(unhex('aa'), 5, unhex('1f2e')));
SELECT hex(rpad(unhex('aa'), 6, unhex('1f2e')));
SELECT hex(rpad(unhex(''), 6, unhex('')));
SELECT hex(rpad(unhex('aabbcc'), 6, unhex('')));
SELECT hex(rpad(unhex('aabbcc'), 2, unhex('ff')));

-- lpad/rpad with mixed STRING and BINARY input
SELECT lpad('abc', 5, x'57');
SELECT lpad(x'57', 5, 'abc');
SELECT rpad('abc', 5, x'57');
SELECT rpad(x'57', 5, 'abc');

-- decode
select decode();
select decode(encode('abc', 'utf-8'));
select decode(encode('abc', 'utf-8'), 'utf-8');
select decode(1, 1, 'Southlake');
select decode(2, 1, 'Southlake');
select decode(2, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle');

-- contains
SELECT CONTAINS(null, 'Spark');
SELECT CONTAINS('Spark SQL', null);
SELECT CONTAINS(null, null);
SELECT CONTAINS('Spark SQL', 'Spark');
SELECT CONTAINS('Spark SQL', 'SQL');
SELECT CONTAINS('Spark SQL', 'SPARK');

SELECT startswith('Spark SQL', 'ark');
SELECT startswith('Spark SQL', 'Spa');
SELECT startswith(null, 'Spark');
SELECT startswith('Spark', null);
SELECT startswith(null, null);

SELECT endswith('Spark SQL', 'QL');
SELECT endswith('Spark SQL', 'Spa');
SELECT endswith(null, 'Spark');
SELECT endswith('Spark', null);
SELECT endswith(null, null);

-- to_number
select to_number('454', '000');
select to_number('454.2', '000.0');
select to_number('12,454', '00,000');
select to_number('$78.12', '$00.00');
select to_number('-454', '-000');
select to_number('-454', 'S000');
select to_number('12,454.8-', '00,000.9-');
select to_number('00,454.8-', '00,000.9-');

-- to_binary
select to_binary('abc');
select to_binary('abc', 'utf-8');
select to_binary('abc', 'base64');
select to_binary('abc', 'base2');
select to_binary('abc', 'hex');
select to_binary('abc', concat('utf', '-8'));
select to_binary('abc', concat('base', '64'));
select to_binary('abc', 'Hex');
select to_binary('abc', 'UTF-8');
select to_binary('abc', null);
select to_binary(null, 'utf-8');
select to_binary(null, null);
select to_binary(null, cast(null as string));
select to_binary('abc', 'invalidFormat');
