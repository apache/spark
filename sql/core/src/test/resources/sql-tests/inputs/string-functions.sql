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
SELECT lpad('abc', 5, x'12');
SELECT lpad(x'12', 5, 'abc');
SELECT rpad('abc', 5, x'12');
SELECT rpad(x'12', 5, 'abc');

-- decode
select decode();
select decode(encode('abc', 'utf-8'));
select decode(encode('abc', 'utf-8'), 'utf-8');
select decode(1, 1, 'Southlake');
select decode(2, 1, 'Southlake');
select decode(2, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle');

-- bitand (2 args)
select bitand(null, null) is null;
select bitand(null, unhex('11223344')) is null;
select bitand(unhex('aabb'), null) is null;
select octet_length(bitand(unhex(''), unhex('')));
select hex(bitand(unhex('aabbccdd'), unhex('11223344')));
-- bitand (3 args; lpad)
select hex(bitand(null, null, 'lpad'));
select octet_length(bitand(unhex(''), unhex(''), 'lpad'));
select hex(bitand(unhex('aabb'), unhex(''), 'lpad'));
select hex(bitand(unhex(''), unhex('11223344'), 'lpad'));
select hex(bitand(unhex('aabb'), unhex('11223344'), 'lpad'));
select hex(bitand(unhex('aabbccdd'), unhex('11223344'), 'lpad'));
select bitand(unhex('aabb'), unhex('11223344'), 'lpad') = bitand(unhex('11223344'), unhex('aabb'), 'lpad');
select bitand(unhex('aabbccdd'), unhex('11223344'), 'lpad') = bitand(unhex('11223344'), unhex('aabbccdd'), 'lpad');
-- bitand (3 args; rpad)
select hex(bitand(null, null, 'lpad'));
select octet_length(bitand(unhex(''), unhex(''), 'rpad'));
select hex(bitand(unhex('aabb'), unhex(''), 'rpad'));
select hex(bitand(unhex(''), unhex('11223344'), 'rpad'));
select hex(bitand(unhex('aabb'), unhex('11223344'), 'rpad'));
select hex(bitand(unhex('aabbccdd'), unhex('11223344'), 'rpad'));
select bitand(unhex('aabb'), unhex('11223344'), 'rpad') = bitand(unhex('11223344'), unhex('aabb'), 'rpad');
select bitand(unhex('aabbccdd'), unhex('11223344'), 'rpad') = bitand(unhex('11223344'), unhex('aabbccdd'), 'rpad');
-- bitand (3 args; common)
select bitand(null, null, null) is null;
select bitand(unhex('aabbccdd'), unhex('11223344'), 'lpad') = bitand(unhex('11223344'), unhex('aabbccdd'), 'rpad');
select bitand(unhex('aabb'), unhex('11223344'), 'lpad') <> bitand(unhex('aabb'), unhex('11223344'), 'rpad');

-- bitnot
select bitnot(null) is null;
select octet_length(bitnot(unhex('')));
select hex(bitnot(unhex('aabb')));

-- bitor (2 args)
select bitor(null, null) is null;
select bitor(null, unhex('11223344')) is null;
select bitor(unhex('aabb'), null) is null;
select octet_length(bitor(unhex(''), unhex('')));
select hex(bitor(unhex('aabbccdd'), unhex('11223344')));
-- bitor (3 args; lpad)
select hex(bitor(null, null, 'lpad'));
select octet_length(bitor(unhex(''), unhex(''), 'lpad'));
select hex(bitor(unhex('aabb'), unhex(''), 'lpad'));
select hex(bitor(unhex(''), unhex('11223344'), 'lpad'));
select hex(bitor(unhex('aabb'), unhex('11223344'), 'lpad'));
select hex(bitor(unhex('aabbccdd'), unhex('11223344'), 'lpad'));
select bitor(unhex('aabb'), unhex('11223344'), 'lpad') = bitor(unhex('11223344'), unhex('aabb'), 'lpad');
select bitor(unhex('aabbccdd'), unhex('11223344'), 'lpad') = bitor(unhex('11223344'), unhex('aabbccdd'), 'lpad');
-- bitor (3 args; rpad)
select hex(bitor(null, null, 'lpad'));
select octet_length(bitor(unhex(''), unhex(''), 'rpad'));
select hex(bitor(unhex('aabb'), unhex(''), 'rpad'));
select hex(bitor(unhex(''), unhex('11223344'), 'rpad'));
select hex(bitor(unhex('aabb'), unhex('11223344'), 'rpad'));
select hex(bitor(unhex('aabbccdd'), unhex('11223344'), 'rpad'));
select bitor(unhex('aabb'), unhex('11223344'), 'rpad') = bitor(unhex('11223344'), unhex('aabb'), 'rpad');
select bitor(unhex('aabbccdd'), unhex('11223344'), 'rpad') = bitor(unhex('11223344'), unhex('aabbccdd'), 'rpad');
-- bitor (3 args; common)
select bitor(null, null, null) is null;
select bitor(unhex('aabbccdd'), unhex('11223344'), 'lpad') = bitor(unhex('11223344'), unhex('aabbccdd'), 'rpad');
select bitor(unhex('aabb'), unhex('11223344'), 'lpad') <> bitor(unhex('aabb'), unhex('11223344'), 'rpad');

-- bitxor (2 args)
select bitxor(null, null) is null;
select bitxor(null, unhex('11223344')) is null;
select bitxor(unhex('aabb'), null) is null;
select octet_length(bitxor(unhex(''), unhex('')));
select hex(bitxor(unhex('aabbccdd'), unhex('11223344')));
-- bitxor (3 args; lpad)
select hex(bitxor(null, null, 'lpad'));
select octet_length(bitxor(unhex(''), unhex(''), 'lpad'));
select hex(bitxor(unhex('aabb'), unhex(''), 'lpad'));
select hex(bitxor(unhex(''), unhex('11223344'), 'lpad'));
select hex(bitxor(unhex('aabb'), unhex('11223344'), 'lpad'));
select hex(bitxor(unhex('aabbccdd'), unhex('11223344'), 'lpad'));
select bitxor(unhex('aabb'), unhex('11223344'), 'lpad') = bitxor(unhex('11223344'), unhex('aabb'), 'lpad');
select bitxor(unhex('aabbccdd'), unhex('11223344'), 'lpad') = bitxor(unhex('11223344'), unhex('aabbccdd'), 'lpad');
-- bitxor (3 args; rpad)
select hex(bitxor(null, null, 'lpad'));
select octet_length(bitxor(unhex(''), unhex(''), 'rpad'));
select hex(bitxor(unhex('aabb'), unhex(''), 'rpad'));
select hex(bitxor(unhex(''), unhex('11223344'), 'rpad'));
select hex(bitxor(unhex('aabb'), unhex('11223344'), 'rpad'));
select hex(bitxor(unhex('aabbccdd'), unhex('11223344'), 'rpad'));
select bitxor(unhex('aabb'), unhex('11223344'), 'rpad') = bitxor(unhex('11223344'), unhex('aabb'), 'rpad');
select bitxor(unhex('aabbccdd'), unhex('11223344'), 'rpad') = bitxor(unhex('11223344'), unhex('aabbccdd'), 'rpad');
-- bitxor (3 args; common)
select bitxor(null, null, null) is null;
select bitxor(unhex('aabbccdd'), unhex('11223344'), 'lpad') = bitxor(unhex('11223344'), unhex('aabbccdd'), 'rpad');
select bitxor(unhex('aabb'), unhex('11223344'), 'lpad') <> bitxor(unhex('aabb'), unhex('11223344'), 'rpad');
