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
SELECT split('hello', '');
SELECT split('', '');
SELECT split('abc', null);
SELECT split(null, 'b');

-- split_part function
SELECT split_part('11.12.13', '.', 2);
SELECT split_part('11.12.13', '.', -1);
SELECT split_part('11.12.13', '.', -3);
SELECT split_part('11.12.13', '', 1);
SELECT split_part('11ab12ab13', 'ab', 1);
SELECT split_part('11.12.13', '.', 0);
SELECT split_part('11.12.13', '.', 4);
SELECT split_part('11.12.13', '.', 5);
SELECT split_part('11.12.13', '.', -5);
SELECT split_part(null, '.', 1);
SELECT split_part(str, delimiter, partNum) FROM VALUES ('11.12.13', '.', 3) AS v1(str, delimiter, partNum);

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
SELECT hex(lpad(unhex('123'), 2));
SELECT hex(lpad(unhex('12345'), 2));
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
SELECT hex(rpad(unhex('123'), 2));
SELECT hex(rpad(unhex('12345'), 2));
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

-- encode
set spark.sql.legacy.javaCharsets=true;
select encode('hello', 'WINDOWS-1252');
select encode(scol, ecol) from values('hello', 'WINDOWS-1252') as t(scol, ecol);
set spark.sql.legacy.javaCharsets=false;
select encode('hello', 'WINDOWS-1252');
select encode(scol, ecol) from values('hello', 'WINDOWS-1252') as t(scol, ecol);
select encode('hello', 'Windows-xxx');
select encode(scol, ecol) from values('hello', 'Windows-xxx') as t(scol, ecol);
set spark.sql.legacy.codingErrorAction=true;
select encode('渭城朝雨浥轻尘', 'US-ASCII');
select encode(scol, ecol) from values('渭城朝雨浥轻尘', 'US-ASCII') as t(scol, ecol);
set spark.sql.legacy.codingErrorAction=false;
select encode('客舍青青柳色新', 'US-ASCII');
select encode(scol, ecol) from values('客舍青青柳色新', 'US-ASCII') as t(scol, ecol);
select encode(decode(encode('白日依山尽，黄河入海流。欲穷千里目，更上一层楼。', 'UTF-16'), 'UTF-16'), 'UTF-8');
select encode(decode(encode('南山經之首曰䧿山。其首曰招搖之山，臨於西海之上。', 'UTF-16'), 'UTF-16'), 'UTF-8');
select encode(decode(encode('세계에서 가장 인기 있는 빅데이터 처리 프레임워크인 Spark', 'UTF-16'), 'UTF-16'), 'UTF-8');
select encode(decode(encode('το Spark είναι το πιο δημοφιλές πλαίσιο επεξεργασίας μεγάλων δεδομένων παγκοσμίως', 'UTF-16'), 'UTF-16'), 'UTF-8');
select encode(decode(encode('Sparkは世界で最も人気のあるビッグデータ処理フレームワークである。', 'UTF-16'), 'UTF-16'), 'UTF-8');

-- decode
select decode();
select decode(encode('abc', 'utf-8'));
select decode(encode('abc', 'utf-8'), 'utf-8');
select decode(encode('大千世界', 'utf-32'), 'utf-32');
select decode(1, 1, 'Southlake');
select decode(2, 1, 'Southlake');
select decode(2, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle', 'Non domestic');
select decode(6, 1, 'Southlake', 2, 'San Francisco', 3, 'New Jersey', 4, 'Seattle');
select decode(null, 6, 'Spark', NULL, 'SQL', 4, 'rocks');
select decode(null, 6, 'Spark', NULL, 'SQL', 4, 'rocks', NULL, '.');
select decode(X'68656c6c6f', 'Windows-xxx');
select decode(scol, ecol) from values(X'68656c6c6f', 'Windows-xxx') as t(scol, ecol);
set spark.sql.legacy.javaCharsets=true;
select decode(X'68656c6c6f', 'WINDOWS-1252');
select decode(scol, ecol) from values(X'68656c6c6f', 'WINDOWS-1252') as t(scol, ecol);
set spark.sql.legacy.javaCharsets=false;
select decode(X'68656c6c6f', 'WINDOWS-1252');
select decode(scol, ecol) from values(X'68656c6c6f', 'WINDOWS-1252') as t(scol, ecol);
set spark.sql.legacy.codingErrorAction=true;
select decode(X'E58A9DE5909BE69BB4E5B0BDE4B880E69DAFE98592', 'US-ASCII');
select decode(scol, ecol) from values(X'E58A9DE5909BE69BB4E5B0BDE4B880E69DAFE98592', 'US-ASCII') as t(scol, ecol);
set spark.sql.legacy.codingErrorAction=false;
select decode(X'E8A5BFE587BAE998B3E585B3E697A0E69585E4BABA', 'US-ASCII');
select decode(scol, ecol) from values(X'E8A5BFE587BAE998B3E585B3E697A0E69585E4BABA', 'US-ASCII') as t(scol, ecol);

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

SELECT contains(x'537061726b2053514c', x'537061726b');
SELECT contains(x'', x'');
SELECT contains(x'537061726b2053514c', null);
SELECT contains(12, '1');
SELECT contains(true, 'ru');
SELECT contains(x'12', 12);
SELECT contains(true, false);

SELECT startswith(x'537061726b2053514c', x'537061726b');
SELECT startswith(x'537061726b2053514c', x'');
SELECT startswith(x'', x'');
SELECT startswith(x'537061726b2053514c', null);

SELECT endswith(x'537061726b2053514c', x'53516c');
SELECT endsWith(x'537061726b2053514c', x'537061726b');
SELECT endsWith(x'537061726b2053514c', x'');
SELECT endsWith(x'', x'');
SELECT endsWith(x'537061726b2053514c', null);

-- to_number
select to_number('454', '000');
select to_number('454.2', '000.0');
select to_number('12,454', '00,000');
select to_number('$78.12', '$00.00');
select to_number('+454', 'S000');
select to_number('-454', 'S000');
select to_number('12,454.8-', '00,000.9MI');
select to_number('00,454.8-', '00,000.9MI');
select to_number('<00,454.8>', '00,000.9PR');

-- to_binary
-- base64 valid
select to_binary('', 'base64');
select to_binary('  ', 'base64');
select to_binary(' ab cd ', 'base64');
select to_binary(' ab c=', 'base64');
select to_binary(' ab cdef= = ', 'base64');
select to_binary(
  concat(' b25lIHR3byB0aHJlZSBmb3VyIGZpdmUgc2l4IHNldmVuIGVpZ2h0IG5pbmUgdGVuIGVsZXZlbiB0',
         'd2VsdmUgdGhpcnRlZW4gZm91cnRlZW4gZml2dGVlbiBzaXh0ZWVuIHNldmVudGVlbiBlaWdodGVl'), 'base64');
-- base64 invalid
select to_binary('a', 'base64');
select to_binary('a?', 'base64');
select to_binary('abcde', 'base64');
select to_binary('abcd=', 'base64');
select to_binary('a===', 'base64');
select to_binary('ab==f', 'base64');
-- utf-8
select to_binary(
  '∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i), ∀x∈ℝ: ⌈x⌉ = −⌊−x⌋, α ∧ ¬β = ¬(¬α ∨ β)', 'utf-8');
select to_binary('大千世界', 'utf8');
select to_binary('', 'utf-8');
select to_binary('  ', 'utf8');
-- hex valid
select to_binary('737472696E67');
select to_binary('737472696E67', 'hex');
select to_binary('');
select to_binary('1', 'hex');
select to_binary('FF');
select to_binary('123', 'hex');
select to_binary('12345', 'hex');
-- hex invalid
select to_binary('GG');
select to_binary('01 AF', 'hex');
-- 'format' parameter can be any foldable string value, not just literal.
select to_binary('abc', concat('utf', '-8'));
select to_binary(' ab cdef= = ', substr('base64whynot', 0, 6));
select to_binary(' ab cdef= = ', replace('HEX0', '0'));
-- 'format' parameter is case insensitive.
select to_binary('abc', 'Hex');
-- null inputs lead to null result.
select to_binary('abc', null);
select to_binary(null, 'utf-8');
select to_binary(null, null);
select to_binary(null, cast(null as string));
-- invalid format
select to_binary('abc', 1);
select to_binary('abc', 'invalidFormat');
CREATE TEMPORARY VIEW fmtTable(fmtField) AS SELECT * FROM VALUES ('invalidFormat');
SELECT to_binary('abc', fmtField) FROM fmtTable;
-- Clean up
DROP VIEW IF EXISTS fmtTable;
-- luhn_check
-- basic cases
select luhn_check('4111111111111111');
select luhn_check('5500000000000004');
select luhn_check('340000000000009');
select luhn_check('6011000000000004');
select luhn_check('6011000000000005');
select luhn_check('378282246310006');
select luhn_check('0');
-- spaces in the beginning/middle/end
select luhn_check('4111111111111111    ');
select luhn_check('4111111 111111111');
select luhn_check(' 4111111111111111');
-- space
select luhn_check('');
select luhn_check('  ');
-- non-digits
select luhn_check('510B105105105106');
select luhn_check('ABCDED');
-- null
select luhn_check(null);
-- non string (test implicit cast)
select luhn_check(6011111111111117);
select luhn_check(6011111111111118);
select luhn_check(123.456);

--utf8 string validation
select is_valid_utf8('');
select is_valid_utf8('abc');
select is_valid_utf8(x'80');
select make_valid_utf8('');
select make_valid_utf8('abc');
select make_valid_utf8(x'80');
select validate_utf8('');
select validate_utf8('abc');
select validate_utf8(x'80');
select try_validate_utf8('');
select try_validate_utf8('abc');
select try_validate_utf8(x'80');
