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

-- decode
select decode();
select decode(encode('abc', 'utf-8'));
select decode(encode('abc', 'utf-8'), 'utf-8');
select decode(1, 1, 'Southlake');
select decode(2, 1, 'Southlake');
select decode(2, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle');

-- bitand
select hex(bitand(null, unhex('11223344')));
select hex(bitand(unhex('aabb'), null));
select hex(bitand(unhex(''), unhex('')));
select hex(bitand(unhex('aabb'), unhex('')));
select hex(bitand(unhex(''), unhex('11223344')));
select hex(bitand(unhex('aabb'), unhex('11223344')));
select hex(bitand(unhex('aabbccdd'), unhex('11223344')));
select bitand(unhex('aabb'), unhex('11223344')) = bitand(unhex('11223344'), unhex('aabb'));
select bitand(unhex('aabbccdd'), unhex('11223344')) = bitand(unhex('11223344'), unhex('aabbccdd'));

-- bitnot
select hex(bitnot(null));
select hex(bitnot(unhex('')));
select hex(bitnot(unhex('aabb')));

-- bitor
select hex(bitor(null, unhex('11223344')));
select hex(bitor(unhex('aabb'), null));
select hex(bitor(unhex(''), unhex('')));
select hex(bitor(unhex('aabb'), unhex('')));
select hex(bitor(unhex(''), unhex('11223344')));
select hex(bitor(unhex('aabb'), unhex('11223344')));
select hex(bitor(unhex('aabbccdd'), unhex('11223344')));
select bitor(unhex('aabb'), unhex('11223344')) = bitor(unhex('11223344'), unhex('aabb'));
select bitor(unhex('aabbccdd'), unhex('11223344')) = bitor(unhex('11223344'), unhex('aabbccdd'));

-- bitxor
select hex(bitxor(null, unhex('11223344')));
select hex(bitxor(unhex('aabb'), null));
select hex(bitxor(unhex(''), unhex('')));
select hex(bitxor(unhex('aabb'), unhex('')));
select hex(bitxor(unhex(''), unhex('11223344')));
select hex(bitxor(unhex('aabb'), unhex('11223344')));
select hex(bitxor(unhex('aabbccdd'), unhex('11223344')));
select bitxor(unhex('aabb'), unhex('11223344')) = bitxor(unhex('11223344'), unhex('aabb'));
select bitxor(unhex('aabbccdd'), unhex('11223344')) = bitxor(unhex('11223344'), unhex('aabbccdd'));