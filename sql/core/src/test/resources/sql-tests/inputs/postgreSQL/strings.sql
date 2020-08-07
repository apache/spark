--
-- Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
--
-- STRINGS
-- -- https://github.com/postgres/postgres/blob/REL_12_BETA2/src/test/regress/sql/strings.sql
-- Test various data entry syntaxes.
--

-- SQL string continuation syntax
-- E021-03 character string literals
SELECT 'first line'
' - next line'
	' - third line'
	AS `Three lines to one`;

-- Spark SQL support this string continuation syntax
-- illegal string continuation syntax
SELECT 'first line'
' - next line' /* this comment is not allowed here */
' - third line'
	AS `Illegal comment within continuation`;

-- [SPARK-28447] ANSI SQL: Unicode escapes in literals
-- Unicode escapes
-- SET standard_conforming_strings TO on;

-- SELECT U&'d\0061t\+000061' AS U&"d\0061t\+000061";
-- SELECT U&'d!0061t\+000061' UESCAPE '!' AS U&"d*0061t\+000061" UESCAPE '*';

-- SELECT U&' \' UESCAPE '!' AS "tricky";
-- SELECT 'tricky' AS U&"\" UESCAPE '!';

-- SELECT U&'wrong: \061';
-- SELECT U&'wrong: \+0061';
-- SELECT U&'wrong: +0061' UESCAPE '+';

-- SET standard_conforming_strings TO off;

-- SELECT U&'d\0061t\+000061' AS U&"d\0061t\+000061";
-- SELECT U&'d!0061t\+000061' UESCAPE '!' AS U&"d*0061t\+000061" UESCAPE '*';

-- SELECT U&' \' UESCAPE '!' AS "tricky";
-- SELECT 'tricky' AS U&"\" UESCAPE '!';

-- SELECT U&'wrong: \061';
-- SELECT U&'wrong: \+0061';
-- SELECT U&'wrong: +0061' UESCAPE '+';

-- RESET standard_conforming_strings;

-- Spark SQL only support escape mode
-- bytea
-- SET bytea_output TO hex;
-- SELECT E'\\xDeAdBeEf'::bytea;
-- SELECT E'\\x De Ad Be Ef '::bytea;
-- SELECT E'\\xDeAdBeE'::bytea;
-- SELECT E'\\xDeAdBeEx'::bytea;
-- SELECT E'\\xDe00BeEf'::bytea;
-- SELECT E'DeAdBeEf'::bytea;
-- SELECT E'De\\000dBeEf'::bytea;
-- SELECT E'De\123dBeEf'::bytea;
-- SELECT E'De\\123dBeEf'::bytea;
-- SELECT E'De\\678dBeEf'::bytea;

-- SET bytea_output TO escape;
SELECT binary('\\xDeAdBeEf');
SELECT binary('\\x De Ad Be Ef ');
SELECT binary('\\xDe00BeEf');
SELECT binary('DeAdBeEf');
SELECT binary('De\\000dBeEf');
SELECT binary('De\\123dBeEf');

-- Skip these tests because we do not have char/varchar type
--
-- test conversions between various string types
-- E021-10 implicit casting among the character data types
--

-- SELECT CAST(f1 AS text) AS "text(char)" FROM CHAR_TBL;

-- SELECT CAST(f1 AS text) AS "text(varchar)" FROM VARCHAR_TBL;

-- SELECT CAST(name 'namefield' AS text) AS "text(name)";

-- since this is an explicit cast, it should truncate w/o error:
-- SELECT CAST(f1 AS char(10)) AS "char(text)" FROM TEXT_TBL;
-- note: implicit-cast case is tested in char.sql

-- SELECT CAST(f1 AS char(20)) AS "char(text)" FROM TEXT_TBL;

-- SELECT CAST(f1 AS char(10)) AS "char(varchar)" FROM VARCHAR_TBL;

-- SELECT CAST(name 'namefield' AS char(10)) AS "char(name)";

-- SELECT CAST(f1 AS varchar) AS "varchar(text)" FROM TEXT_TBL;

-- SELECT CAST(f1 AS varchar) AS "varchar(char)" FROM CHAR_TBL;

-- SELECT CAST(name 'namefield' AS varchar) AS "varchar(name)";

--
-- test SQL string functions
-- E### and T### are feature reference numbers from SQL99
--

-- E021-09 trim function
SELECT TRIM(BOTH FROM '  bunch o blanks  ') = 'bunch o blanks' AS `bunch o blanks`;

SELECT TRIM(LEADING FROM '  bunch o blanks  ') = 'bunch o blanks  ' AS `bunch o blanks  `;

SELECT TRIM(TRAILING FROM '  bunch o blanks  ') = '  bunch o blanks' AS `  bunch o blanks`;

SELECT TRIM(BOTH 'x' FROM 'xxxxxsome Xsxxxxx') = 'some Xs' AS `some Xs`;

-- E021-06 substring expression
SELECT SUBSTRING('1234567890' FROM 3) = '34567890' AS `34567890`;

SELECT SUBSTRING('1234567890' FROM 4 FOR 3) = '456' AS `456`;

-- [SPARK-28076] Support regular expression substring
-- T581 regular expression substring (with SQL's bizarre regexp syntax)
-- SELECT SUBSTRING('abcdefg' FROM 'a#"(b_d)#"%' FOR '#') AS "bcd";

-- No match should return NULL
-- SELECT SUBSTRING('abcdefg' FROM '#"(b_d)#"%' FOR '#') IS NULL AS "True";

-- Null inputs should return NULL
-- SELECT SUBSTRING('abcdefg' FROM '%' FOR NULL) IS NULL AS "True";
-- SELECT SUBSTRING(NULL FROM '%' FOR '#') IS NULL AS "True";
-- SELECT SUBSTRING('abcdefg' FROM NULL FOR '#') IS NULL AS "True";

-- The first and last parts should act non-greedy
-- SELECT SUBSTRING('abcdefg' FROM 'a#"%#"g' FOR '#') AS "bcdef";
-- SELECT SUBSTRING('abcdefg' FROM 'a*#"%#"g*' FOR '#') AS "abcdefg";

-- Vertical bar in any part affects only that part
-- SELECT SUBSTRING('abcdefg' FROM 'a|b#"%#"g' FOR '#') AS "bcdef";
-- SELECT SUBSTRING('abcdefg' FROM 'a#"%#"x|g' FOR '#') AS "bcdef";
-- SELECT SUBSTRING('abcdefg' FROM 'a#"%|ab#"g' FOR '#') AS "bcdef";

-- Can't have more than two part separators
-- SELECT SUBSTRING('abcdefg' FROM 'a*#"%#"g*#"x' FOR '#') AS "error";

-- Postgres extension: with 0 or 1 separator, assume parts 1 and 3 are empty
-- SELECT SUBSTRING('abcdefg' FROM 'a#"%g' FOR '#') AS "bcdefg";
-- SELECT SUBSTRING('abcdefg' FROM 'a%g' FOR '#') AS "abcdefg";

-- substring() with just two arguments is not allowed by SQL spec;
-- we accept it, but we interpret the pattern as a POSIX regexp not SQL
-- SELECT SUBSTRING('abcdefg' FROM 'c.e') AS "cde";

-- With a parenthesized subexpression, return only what matches the subexpr
-- SELECT SUBSTRING('abcdefg' FROM 'b(.*)f') AS "cde";

-- [SPARK-27952] String Functions: regexp_replace is not compatible
-- PostgreSQL extension to allow using back reference in replace string;
-- SELECT regexp_replace('1112223333', E'(\\d{3})(\\d{3})(\\d{4})', E'(\\1) \\2-\\3');
-- SELECT regexp_replace('AAA   BBB   CCC   ', E'\\s+', ' ', 'g');
-- SELECT regexp_replace('AAA', '^|$', 'Z', 'g');
-- SELECT regexp_replace('AAA aaa', 'A+', 'Z', 'gi');
-- invalid regexp option
-- SELECT regexp_replace('AAA aaa', 'A+', 'Z', 'z');

-- set so we can tell NULL from empty string
-- \pset null '\\N'

-- [SPARK-28078] Add support other 4 REGEXP functions
-- return all matches from regexp
-- SELECT regexp_matches('foobarbequebaz', $re$(bar)(beque)$re$);

-- test case insensitive
-- SELECT regexp_matches('foObARbEqUEbAz', $re$(bar)(beque)$re$, 'i');

-- global option - more than one match
-- SELECT regexp_matches('foobarbequebazilbarfbonk', $re$(b[^b]+)(b[^b]+)$re$, 'g');

-- empty capture group (matched empty string)
-- SELECT regexp_matches('foobarbequebaz', $re$(bar)(.*)(beque)$re$);
-- no match
-- SELECT regexp_matches('foobarbequebaz', $re$(bar)(.+)(beque)$re$);
-- optional capture group did not match, null entry in array
-- SELECT regexp_matches('foobarbequebaz', $re$(bar)(.+)?(beque)$re$);

-- no capture groups
-- SELECT regexp_matches('foobarbequebaz', $re$barbeque$re$);

-- start/end-of-line matches are of zero length
-- SELECT regexp_matches('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '^', 'mg');
-- SELECT regexp_matches('foo' || chr(10) || 'bar' || chr(10) || 'bequq' || chr(10) || 'baz', '$', 'mg');
-- SELECT regexp_matches('1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4' || chr(10), '^.?', 'mg');
-- SELECT regexp_matches(chr(10) || '1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4' || chr(10), '.?$', 'mg');
-- SELECT regexp_matches(chr(10) || '1' || chr(10) || '2' || chr(10) || '3' || chr(10) || '4', '.?$', 'mg');

-- give me errors
-- SELECT regexp_matches('foobarbequebaz', $re$(bar)(beque)$re$, 'gz');
-- SELECT regexp_matches('foobarbequebaz', $re$(barbeque$re$);
-- SELECT regexp_matches('foobarbequebaz', $re$(bar)(beque){2,1}$re$);

-- split string on regexp
-- SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', $re$\s+$re$) AS foo;
-- SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', $re$\s+$re$);

-- SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', $re$\s*$re$) AS foo;
-- SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', $re$\s*$re$);
-- SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', '') AS foo;
-- SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', '');
-- case insensitive
-- SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'i') AS foo;
-- -- SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'i');
-- no match of pattern
-- SELECT foo, length(foo) FROM regexp_split_to_table('the quick brown fox jumps over the lazy dog', 'nomatch') AS foo;
-- SELECT regexp_split_to_array('the quick brown fox jumps over the lazy dog', 'nomatch');
-- some corner cases
-- SELECT regexp_split_to_array('123456','1');
-- SELECT regexp_split_to_array('123456','6');
-- SELECT regexp_split_to_array('123456','.');
-- SELECT regexp_split_to_array('123456','');
-- SELECT regexp_split_to_array('123456','(?:)');
-- SELECT regexp_split_to_array('1','');
-- errors
-- SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'zippy') AS foo;
-- SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'iz');
-- global option meaningless for regexp_split
-- SELECT foo, length(foo) FROM regexp_split_to_table('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'g') AS foo;
-- SELECT regexp_split_to_array('thE QUick bROWn FOx jUMPs ovEr The lazy dOG', 'e', 'g');

-- change NULL-display back
-- \pset null ''

-- E021-11 position expression
SELECT POSITION('4' IN '1234567890') = '4' AS `4`;

SELECT POSITION('5' IN '1234567890') = '5' AS `5`;

-- [SPARK-28077] Add support string functions: OVERLAY
-- T312 character overlay function
SELECT OVERLAY('abcdef' PLACING '45' FROM 4) AS `abc45f`;

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5) AS `yabadaba`;

SELECT OVERLAY('yabadoo' PLACING 'daba' FROM 5 FOR 0) AS `yabadabadoo`;

SELECT OVERLAY('babosa' PLACING 'ubb' FROM 2 FOR 4) AS `bubba`;

--
-- test LIKE
-- Be sure to form every test as a LIKE/NOT LIKE pair.
--

-- simplest examples
-- E061-04 like predicate
SELECT 'hawkeye' LIKE 'h%' AS `true`;
SELECT 'hawkeye' NOT LIKE 'h%' AS `false`;

SELECT 'hawkeye' LIKE 'H%' AS `false`;
SELECT 'hawkeye' NOT LIKE 'H%' AS `true`;

SELECT 'hawkeye' LIKE 'indio%' AS `false`;
SELECT 'hawkeye' NOT LIKE 'indio%' AS `true`;

SELECT 'hawkeye' LIKE 'h%eye' AS `true`;
SELECT 'hawkeye' NOT LIKE 'h%eye' AS `false`;

SELECT 'indio' LIKE '_ndio' AS `true`;
SELECT 'indio' NOT LIKE '_ndio' AS `false`;

SELECT 'indio' LIKE 'in__o' AS `true`;
SELECT 'indio' NOT LIKE 'in__o' AS `false`;

SELECT 'indio' LIKE 'in_o' AS `false`;
SELECT 'indio' NOT LIKE 'in_o' AS `true`;

-- unused escape character
SELECT 'hawkeye' LIKE 'h%' ESCAPE '#' AS `true`;
SELECT 'hawkeye' NOT LIKE 'h%' ESCAPE '#' AS `false`;

SELECT 'indio' LIKE 'ind_o' ESCAPE '$' AS `true`;
SELECT 'indio' NOT LIKE 'ind_o' ESCAPE '$' AS `false`;

-- escape character
-- E061-05 like predicate with escape clause
SELECT 'h%' LIKE 'h#%' ESCAPE '#' AS `true`;
SELECT 'h%' NOT LIKE 'h#%' ESCAPE '#' AS `false`;

SELECT 'h%wkeye' LIKE 'h#%' ESCAPE '#' AS `false`;
SELECT 'h%wkeye' NOT LIKE 'h#%' ESCAPE '#' AS `true`;

SELECT 'h%wkeye' LIKE 'h#%%' ESCAPE '#' AS `true`;
SELECT 'h%wkeye' NOT LIKE 'h#%%' ESCAPE '#' AS `false`;

SELECT 'h%awkeye' LIKE 'h#%a%k%e' ESCAPE '#' AS `true`;
SELECT 'h%awkeye' NOT LIKE 'h#%a%k%e' ESCAPE '#' AS `false`;

SELECT 'indio' LIKE '_ndio' ESCAPE '$' AS `true`;
SELECT 'indio' NOT LIKE '_ndio' ESCAPE '$' AS `false`;

SELECT 'i_dio' LIKE 'i$_d_o' ESCAPE '$' AS `true`;
SELECT 'i_dio' NOT LIKE 'i$_d_o' ESCAPE '$' AS `false`;

SELECT 'i_dio' LIKE 'i$_nd_o' ESCAPE '$' AS `false`;
SELECT 'i_dio' NOT LIKE 'i$_nd_o' ESCAPE '$' AS `true`;

SELECT 'i_dio' LIKE 'i$_d%o' ESCAPE '$' AS `true`;
SELECT 'i_dio' NOT LIKE 'i$_d%o' ESCAPE '$' AS `false`;

-- escape character same as pattern character
SELECT 'maca' LIKE 'm%aca' ESCAPE '%' AS `true`;
SELECT 'maca' NOT LIKE 'm%aca' ESCAPE '%' AS `false`;

SELECT 'ma%a' LIKE 'm%a%%a' ESCAPE '%' AS `true`;
SELECT 'ma%a' NOT LIKE 'm%a%%a' ESCAPE '%' AS `false`;

SELECT 'bear' LIKE 'b_ear' ESCAPE '_' AS `true`;
SELECT 'bear' NOT LIKE 'b_ear' ESCAPE '_' AS `false`;

SELECT 'be_r' LIKE 'b_e__r' ESCAPE '_' AS `true`;
SELECT 'be_r' NOT LIKE 'b_e__r' ESCAPE '_' AS `false`;

SELECT 'be_r' LIKE '__e__r' ESCAPE '_' AS `false`;
SELECT 'be_r' NOT LIKE '__e__r' ESCAPE '_' AS `true`;

-- [SPARK-28448] Implement ILIKE operator
--
-- test ILIKE (case-insensitive LIKE)
-- Be sure to form every test as an ILIKE/NOT ILIKE pair.
--

-- SELECT 'hawkeye' ILIKE 'h%' AS "true";
-- SELECT 'hawkeye' NOT ILIKE 'h%' AS "false";

-- SELECT 'hawkeye' ILIKE 'H%' AS "true";
-- SELECT 'hawkeye' NOT ILIKE 'H%' AS "false";

-- SELECT 'hawkeye' ILIKE 'H%Eye' AS "true";
-- SELECT 'hawkeye' NOT ILIKE 'H%Eye' AS "false";

-- SELECT 'Hawkeye' ILIKE 'h%' AS "true";
-- SELECT 'Hawkeye' NOT ILIKE 'h%' AS "false";

--
-- test %/_ combination cases, cf bugs #4821 and #5478
--

SELECT 'foo' LIKE '_%' as t, 'f' LIKE '_%' as t, '' LIKE '_%' as f;
SELECT 'foo' LIKE '%_' as t, 'f' LIKE '%_' as t, '' LIKE '%_' as f;

SELECT 'foo' LIKE '__%' as t, 'foo' LIKE '___%' as t, 'foo' LIKE '____%' as f;
SELECT 'foo' LIKE '%__' as t, 'foo' LIKE '%___' as t, 'foo' LIKE '%____' as f;

SELECT 'jack' LIKE '%____%' AS t;


-- Skip tests of LIKE with indexes
--
-- basic tests of LIKE with indexes
--

-- CREATE TABLE texttest (a text PRIMARY KEY, b int);
-- SELECT * FROM texttest WHERE a LIKE '%1%';

-- CREATE TABLE byteatest (a bytea PRIMARY KEY, b int);
-- SELECT * FROM byteatest WHERE a LIKE '%1%';

-- DROP TABLE texttest, byteatest;


--
-- test implicit type conversion
--

-- E021-07 character concatenation
SELECT 'unknown' || ' and unknown' AS `Concat unknown types`;

SELECT string('text') || ' and unknown' AS `Concat text to unknown type`;

-- Spark SQL does not have char and varchar type
-- SELECT char(20) 'characters' || ' and text' AS "Concat char to unknown type";

-- SELECT text 'text' || char(20) ' and characters' AS "Concat text to char";

-- SELECT text 'text' || varchar ' and varchar' AS "Concat text to varchar";

--
-- test substr with toasted text values
--
CREATE TABLE toasttest(f1 string) USING parquet;

insert into toasttest values(repeat('1234567890',10000));
insert into toasttest values(repeat('1234567890',10000));

--
-- Ensure that some values are uncompressed, to test the faster substring
-- operation used in that case
--
-- alter table toasttest alter column f1 set storage external;
insert into toasttest values(repeat('1234567890',10000));
insert into toasttest values(repeat('1234567890',10000));

-- [SPARK-28451] substr returns different values
-- If the starting position is zero or less, then return from the start of the string
-- adjusting the length to be consistent with the "negative start" per SQL.
-- SELECT substr(f1, -1, 5) from toasttest;

-- If the length is less than zero, an ERROR is thrown.
-- SELECT substr(f1, 5, -1) from toasttest;

-- If no third argument (length) is provided, the length to the end of the
-- string is assumed.
SELECT substr(f1, 99995) from toasttest;

-- If start plus length is > string length, the result is truncated to
-- string length
SELECT substr(f1, 99995, 10) from toasttest;

-- Skip these tests
-- TRUNCATE TABLE toasttest;
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- expect >0 blocks
-- SELECT pg_relation_size(reltoastrelid) = 0 AS is_empty
--  FROM pg_class where relname = 'toasttest';

-- TRUNCATE TABLE toasttest;
-- ALTER TABLE toasttest set (toast_tuple_target = 4080);
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- INSERT INTO toasttest values (repeat('1234567890',300));
-- expect 0 blocks
-- SELECT pg_relation_size(reltoastrelid) = 0 AS is_empty
--  FROM pg_class where relname = 'toasttest';

-- DROP TABLE toasttest;

-- [SPARK-28121] decode can not accept 'escape' as charset
--
--
-- test substr with toasted bytea values
--
-- CREATE TABLE toasttest(f1 binary) USING parquet;

-- insert into toasttest values(decode(repeat('1234567890',10000),'escape'));
-- insert into toasttest values(decode(repeat('1234567890',10000),'escape'));

--
-- Ensure that some values are uncompressed, to test the faster substring
-- operation used in that case
--
-- alter table toasttest alter column f1 set storage external;
-- insert into toasttest values(decode(repeat('1234567890',10000),'escape'));
-- insert into toasttest values(decode(repeat('1234567890',10000),'escape'));

-- If the starting position is zero or less, then return from the start of the string
-- adjusting the length to be consistent with the "negative start" per SQL.
-- SELECT substr(f1, -1, 5) from toasttest;

-- If the length is less than zero, an ERROR is thrown.
-- SELECT substr(f1, 5, -1) from toasttest;

-- If no third argument (length) is provided, the length to the end of the
-- string is assumed.
-- SELECT substr(f1, 99995) from toasttest;

-- If start plus length is > string length, the result is truncated to
-- string length
-- SELECT substr(f1, 99995, 10) from toasttest;

-- DROP TABLE toasttest;

-- Skip these tests because we do not support char type
-- test internally compressing datums

-- this tests compressing a datum to a very small size which exercises a
-- corner case in packed-varlena handling: even though small, the compressed
-- datum must be given a 4-byte header because there are no bits to indicate
-- compression in a 1-byte header

-- CREATE TABLE toasttest (c char(4096));
-- INSERT INTO toasttest VALUES('x');
-- SELECT length(c), c::text FROM toasttest;
-- SELECT c FROM toasttest;
-- DROP TABLE toasttest;

--
-- test length
--

SELECT length('abcdef') AS `length_6`;

-- [SPARK-27930] Replace strpos with locate or position in Spark SQL
--
-- test strpos
--

SELECT position('cd', 'abcdef') AS `pos_3`;

SELECT position('xy', 'abcdef') AS `pos_0`;

--
-- test replace
--
SELECT replace('abcdef', 'de', '45') AS `abc45f`;

SELECT replace('yabadabadoo', 'ba', '123') AS `ya123da123doo`;

SELECT replace('yabadoo', 'bad', '') AS `yaoo`;

-- [SPARK-28087] Add support split_part
--
-- test split_part
--
-- select split_part('joeuser@mydatabase','@',0) AS "an error";

-- select split_part('joeuser@mydatabase','@',1) AS "joeuser";

-- select split_part('joeuser@mydatabase','@',2) AS "mydatabase";

-- select split_part('joeuser@mydatabase','@',3) AS "empty string";

-- select split_part('@joeuser@mydatabase@','@',2) AS "joeuser";

-- [SPARK-27930] Spark SQL use hex
--
-- test to_hex
--
select hex(256*256*256 - 1) AS `ffffff`;

select hex(bigint(bigint(bigint(bigint(256)*256)*256)*256) - 1) AS `ffffffff`;

--
-- MD5 test suite - from IETF RFC 1321
-- (see: ftp://ftp.rfc-editor.org/in-notes/rfc1321.txt)
--
select md5('') = 'd41d8cd98f00b204e9800998ecf8427e' AS `TRUE`;

select md5('a') = '0cc175b9c0f1b6a831c399e269772661' AS `TRUE`;

select md5('abc') = '900150983cd24fb0d6963f7d28e17f72' AS `TRUE`;

select md5('message digest') = 'f96b697d7cb7938d525a2f31aaf161d0' AS `TRUE`;

select md5('abcdefghijklmnopqrstuvwxyz') = 'c3fcd3d76192e4007dfb496cca67e13b' AS `TRUE`;

select md5('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') = 'd174ab98d277d9f5a5611c2c9f419d9f' AS `TRUE`;

select md5('12345678901234567890123456789012345678901234567890123456789012345678901234567890') = '57edf4a22be3c955ac49da2e2107b67a' AS `TRUE`;

select md5(binary('')) = 'd41d8cd98f00b204e9800998ecf8427e' AS `TRUE`;

select md5(binary('a')) = '0cc175b9c0f1b6a831c399e269772661' AS `TRUE`;

select md5(binary('abc')) = '900150983cd24fb0d6963f7d28e17f72' AS `TRUE`;

select md5(binary('message digest')) = 'f96b697d7cb7938d525a2f31aaf161d0' AS `TRUE`;

select md5(binary('abcdefghijklmnopqrstuvwxyz')) = 'c3fcd3d76192e4007dfb496cca67e13b' AS `TRUE`;

select md5(binary('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')) = 'd174ab98d277d9f5a5611c2c9f419d9f' AS `TRUE`;

select md5(binary('12345678901234567890123456789012345678901234567890123456789012345678901234567890')) = '57edf4a22be3c955ac49da2e2107b67a' AS `TRUE`;

-- [SPARK-28122] missing SHA functions
--
-- SHA-2
--
-- SET bytea_output TO hex;

-- SELECT sha224('');
-- SELECT sha224('The quick brown fox jumps over the lazy dog.');

-- SELECT sha256('');
-- SELECT sha256('The quick brown fox jumps over the lazy dog.');

-- SELECT sha384('');
-- SELECT sha384('The quick brown fox jumps over the lazy dog.');

-- SELECT sha512('');
-- SELECT sha512('The quick brown fox jumps over the lazy dog.');

-- [SPARK-28449] Missing escape_string_warning and standard_conforming_strings config
--
-- test behavior of escape_string_warning and standard_conforming_strings options
--
-- set escape_string_warning = off;
-- set standard_conforming_strings = off;

-- show escape_string_warning;
-- show standard_conforming_strings;

-- set escape_string_warning = on;
-- set standard_conforming_strings = on;

-- show escape_string_warning;
-- show standard_conforming_strings;

-- select 'a\bcd' as f1, 'a\b''cd' as f2, 'a\b''''cd' as f3, 'abcd\'   as f4, 'ab\''cd' as f5, '\\' as f6;

-- set standard_conforming_strings = off;

-- select 'a\\bcd' as f1, 'a\\b\'cd' as f2, 'a\\b\'''cd' as f3, 'abcd\\'   as f4, 'ab\\\'cd' as f5, '\\\\' as f6;

-- set escape_string_warning = off;
-- set standard_conforming_strings = on;

-- select 'a\bcd' as f1, 'a\b''cd' as f2, 'a\b''''cd' as f3, 'abcd\'   as f4, 'ab\''cd' as f5, '\\' as f6;

-- set standard_conforming_strings = off;

-- select 'a\\bcd' as f1, 'a\\b\'cd' as f2, 'a\\b\'''cd' as f3, 'abcd\\'   as f4, 'ab\\\'cd' as f5, '\\\\' as f6;


--
-- Additional string functions
--
-- SET bytea_output TO escape;

SELECT initcap('hi THOMAS');

SELECT lpad('hi', 5, 'xy');
SELECT lpad('hi', 5);
SELECT lpad('hi', -5, 'xy');
SELECT lpad('hello', 2);
SELECT lpad('hi', 5, '');

SELECT rpad('hi', 5, 'xy');
SELECT rpad('hi', 5);
SELECT rpad('hi', -5, 'xy');
SELECT rpad('hello', 2);
SELECT rpad('hi', 5, '');

-- skip this test because PostgreSQL has different parameter order compares to SparkSQL
-- SELECT ltrim('zzzytrim', 'xyz');

SELECT translate('', '14', 'ax');
SELECT translate('12345', '14', 'ax');

SELECT ascii('x');
SELECT ascii('');

SELECT chr(65);
-- PostgreSQL throws: ERROR:  null character not permitted
SELECT chr(0);

SELECT repeat('Pg', 4);
SELECT repeat('Pg', -4);

SELECT trim(binary('\\000') from binary('\\000Tom\\000'));
-- [SPARK-28123] Add support btrim
-- SELECT btrim(E'\\000trim\\000'::bytea, E'\\000'::bytea);
-- SELECT btrim(''::bytea, E'\\000'::bytea);
-- SELECT btrim(E'\\000trim\\000'::bytea, ''::bytea);
-- [SPARK-28121] decode can not accept 'escape' as charset
-- [SPARK-28412][SQL] ANSI SQL: OVERLAY function support byte array
-- SELECT encode(overlay(E'Th\\000omas'::bytea placing E'Th\\001omas'::bytea from 2),'escape');
-- SELECT encode(overlay(E'Th\\000omas'::bytea placing E'\\002\\003'::bytea from 8),'escape');
-- SELECT encode(overlay(E'Th\\000omas'::bytea placing E'\\002\\003'::bytea from 5 for 3),'escape');

DROP TABLE toasttest;
