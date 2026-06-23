-- test cases for collation support

-- Clean up any leftover tables from previous runs (e.g. shared warehouse across config dimensions)
drop table if exists t2;
drop table if exists t1;

-- Create a test table with data
create table t1(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values('aaa', 'aaa');
insert into t1 values('AAA', 'AAA');
insert into t1 values('bbb', 'bbb');
insert into t1 values('BBB', 'BBB');

-- describe
describe table t1;

-- group by and count utf8_binary
select count(*) from t1 group by utf8_binary;

-- group by and count utf8_lcase
select count(*) from t1 group by utf8_lcase;

-- filter equal utf8_binary
select * from t1 where utf8_binary = 'aaa';

-- filter equal utf8_lcase
select * from t1 where utf8_lcase = 'aaa' collate utf8_lcase;

-- filter less then utf8_binary
select * from t1 where utf8_binary < 'bbb';

-- filter less then utf8_lcase
select * from t1 where utf8_lcase < 'bbb' collate utf8_lcase;

-- inner join
select l.utf8_binary, r.utf8_lcase from t1 l join t1 r on l.utf8_lcase = r.utf8_lcase;

-- create second table for anti-join
create table t2(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t2 values('aaa', 'aaa');
insert into t2 values('bbb', 'bbb');

-- anti-join on lcase
select * from t1 anti join t2 on t1.utf8_lcase = t2.utf8_lcase;

drop table t2;
drop table t1;

-- set operations
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') except select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') except all select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') union select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') union all select col1 collate utf8_lcase from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') intersect select col1 collate utf8_lcase from values ('aaa'), ('bbb');

-- set operations with conflicting collations
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') except select col1 collate unicode_ci from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') except all select col1 collate unicode_ci from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') union select col1 collate unicode_ci from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('AAA'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') union all select col1 collate unicode_ci from values ('aaa'), ('bbb');
select col1 collate utf8_lcase from values ('aaa'), ('bbb'), ('BBB'), ('zzz'), ('ZZZ') intersect select col1 collate unicode_ci from values ('aaa'), ('bbb');

-- create table with struct field
create table t1 (c1 struct<utf8_binary: string collate utf8_binary, utf8_lcase: string collate utf8_lcase>) USING PARQUET;

insert into t1 values (named_struct('utf8_binary', 'aaa', 'utf8_lcase', 'aaa'));
insert into t1 values (named_struct('utf8_binary', 'AAA', 'utf8_lcase', 'AAA'));

-- aggregate against nested field utf8_binary
select count(*) from t1 group by c1.utf8_binary;

-- aggregate against nested field utf8_lcase
select count(*) from t1 group by c1.utf8_lcase;

drop table t1;

-- array function tests
select array_contains(ARRAY('aaa' collate utf8_lcase),'AAA' collate utf8_lcase);
select array_position(ARRAY('aaa' collate utf8_lcase, 'bbb' collate utf8_lcase),'BBB' collate utf8_lcase);

-- utility
select nullif('aaa' COLLATE utf8_lcase, 'AAA' COLLATE utf8_lcase);
select least('aaa' COLLATE utf8_lcase, 'AAA' collate utf8_lcase, 'a' collate utf8_lcase);

-- array operations
select arrays_overlap(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));
select array_distinct(array('aaa' collate utf8_lcase, 'AAA' collate utf8_lcase));
select array_union(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));
select array_intersect(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));
select array_except(array('aaa' collate utf8_lcase), array('AAA' collate utf8_lcase));

-- ICU collations (all statements return true)
select 'a' collate unicode < 'A';
select 'a' collate unicode_ci = 'A';
select 'a' collate unicode_ai = 'å';
select 'a' collate unicode_ci_ai = 'Å';
select 'a' collate en < 'A';
select 'a' collate en_ci = 'A';
select 'a' collate en_ai = 'å';
select 'a' collate en_ci_ai = 'Å';
select 'Kypper' collate sv < 'Köpfe';
select 'Kypper' collate de > 'Köpfe';
select 'I' collate tr_ci = 'ı';

-- create table for str_to_map
create table t3 (text string collate utf8_binary, pairDelim string collate utf8_lcase, keyValueDelim string collate utf8_binary) using parquet;

insert into t3 values('a:1,b:2,c:3', ',', ':');

select str_to_map(text, pairDelim, keyValueDelim) from t3;
select str_to_map(text collate utf8_binary, pairDelim collate utf8_lcase, keyValueDelim collate utf8_binary) from t3;
select str_to_map(text collate utf8_binary, pairDelim collate utf8_binary, keyValueDelim collate utf8_binary) from t3;
select str_to_map(text collate unicode_ai, pairDelim collate unicode_ai, keyValueDelim collate unicode_ai) from t3;

drop table t3;

create table t1(s string, utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t1 values ('Spark', 'Spark', 'SQL');
insert into t1 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaAAaA');
insert into t1 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaaAaA');
insert into t1 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaaAaAaaAaaAaAaaAaaAaA');
insert into t1 values ('bbAbaAbA', 'bbAbAAbA', 'a');
insert into t1 values ('İo', 'İo', 'İo');
insert into t1 values ('İo', 'İo', 'İo ');
insert into t1 values ('İo', 'İo ', 'İo');
insert into t1 values ('İo', 'İo', 'i̇o');
insert into t1 values ('efd2', 'efd2', 'efd2');
insert into t1 values ('Hello, world! Nice day.', 'Hello, world! Nice day.', 'Hello, world! Nice day.');
insert into t1 values ('Something else. Nothing here.', 'Something else. Nothing here.', 'Something else. Nothing here.');
insert into t1 values ('kitten', 'kitten', 'sitTing');
insert into t1 values ('abc', 'abc', 'abc');
insert into t1 values ('abcdcba', 'abcdcba', 'aBcDCbA');

create table t2(ascii double) using parquet;
insert into t2 values (97.52143);
insert into t2 values (66.421);

create table t3(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t3 values ('aaAaAAaA', 'aaAaaAaA');
insert into t3 values ('efd2', 'efd2');

create table t4(num long) using parquet;
insert into t4 values (97);
insert into t4 values (66);

-- Elt
select elt(2, s, utf8_binary) from t1;
select elt(2, utf8_binary, utf8_lcase, s) from t1;
select elt(1, utf8_binary collate utf8_binary, utf8_lcase collate utf8_lcase) from t1;
select elt(1, utf8_binary collate utf8_binary, utf8_lcase collate utf8_binary) from t1;
select elt(1, utf8_binary collate utf8_binary, utf8_lcase) from t1;
select elt(1, utf8_binary, 'word'), elt(1, utf8_lcase, 'word') from t1;
select elt(1, utf8_binary, 'word' collate utf8_lcase), elt(1, utf8_lcase, 'word' collate utf8_binary) from t1;

-- Ascii & UnBase64 string expressions
select ascii(utf8_binary), ascii(utf8_lcase) from t1;
select ascii(utf8_binary collate utf8_lcase), ascii(utf8_lcase collate utf8_binary) from t1;
select unbase64(utf8_binary), unbase64(utf8_lcase) from t3;
select unbase64(utf8_binary collate utf8_lcase), unbase64(utf8_lcase collate utf8_binary) from t3;

-- Base64, Decode
select base64(utf8_binary), base64(utf8_lcase) from t1;
select base64(utf8_binary collate utf8_lcase), base64(utf8_lcase collate utf8_binary) from t1;
select decode(encode(utf8_binary, 'utf-8'), 'utf-8'), decode(encode(utf8_lcase, 'utf-8'), 'utf-8') from t1;
select decode(encode(utf8_binary collate utf8_lcase, 'utf-8'), 'utf-8'), decode(encode(utf8_lcase collate utf8_binary, 'utf-8'), 'utf-8') from t1;

-- FormatNumber
select format_number(ascii, '###.###') from t2;
select format_number(ascii, '###.###' collate utf8_lcase) from t2;

-- Encode, ToBinary
select encode(utf8_binary, 'utf-8'), encode(utf8_lcase, 'utf-8') from t1;
select encode(utf8_binary collate utf8_lcase, 'utf-8'), encode(utf8_lcase collate utf8_binary, 'utf-8') from t1;
select to_binary(utf8_binary, 'utf-8'), to_binary(utf8_lcase, 'utf-8') from t1;
select to_binary(utf8_binary collate utf8_lcase, 'utf-8'), to_binary(utf8_lcase collate utf8_binary, 'utf-8') from t1;

-- SoundEx
select soundex(utf8_binary), soundex(utf8_lcase) from t1;
select soundex(utf8_binary collate utf8_lcase), soundex(utf8_lcase collate utf8_binary) from t1;

-- Luhncheck
select luhn_check(num) from t4;

-- Levenshtein
select levenshtein(utf8_binary, utf8_lcase) from t1;
select levenshtein(s, utf8_binary) from t1;
select levenshtein(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select levenshtein(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select levenshtein(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select levenshtein(utf8_binary, 'a'), levenshtein(utf8_lcase, 'a') from t1;
select levenshtein(utf8_binary, 'AaAA' collate utf8_lcase, 3), levenshtein(utf8_lcase, 'AAa' collate utf8_binary, 4) from t1;

-- IsValidUTF8
select is_valid_utf8(utf8_binary), is_valid_utf8(utf8_lcase) from t1;
select is_valid_utf8(utf8_binary collate utf8_lcase), is_valid_utf8(utf8_lcase collate utf8_binary) from t1;
select is_valid_utf8(utf8_binary collate utf8_lcase_rtrim), is_valid_utf8(utf8_lcase collate utf8_binary_rtrim) from t1;

-- MakeValidUTF8
select make_valid_utf8(utf8_binary), make_valid_utf8(utf8_lcase) from t1;
select make_valid_utf8(utf8_binary collate utf8_lcase), make_valid_utf8(utf8_lcase collate utf8_binary) from t1;
select make_valid_utf8(utf8_binary collate utf8_lcase_rtrim), make_valid_utf8(utf8_lcase collate utf8_binary_rtrim) from t1;

-- ValidateUTF8
select validate_utf8(utf8_binary), validate_utf8(utf8_lcase) from t1;
select validate_utf8(utf8_binary collate utf8_lcase), validate_utf8(utf8_lcase collate utf8_binary) from t1;
select validate_utf8(utf8_binary collate utf8_lcase_rtrim), validate_utf8(utf8_lcase collate utf8_binary_rtrim) from t1;

-- TryValidateUTF8
select try_validate_utf8(utf8_binary), try_validate_utf8(utf8_lcase) from t1;
select try_validate_utf8(utf8_binary collate utf8_lcase), try_validate_utf8(utf8_lcase collate utf8_binary) from t1;
select try_validate_utf8(utf8_binary collate utf8_lcase_rtrim), try_validate_utf8(utf8_lcase collate utf8_binary_rtrim) from t1;

-- Apply CollationTypeCoercion to condition expressions
SELECT CASE WHEN utf8_lcase = 'XX' THEN 'XX' ELSE utf8_lcase END FROM t1;

drop table t1;
drop table t2;
drop table t3;
drop table t4;

-- ============================================================================
-- Collate returns proper type
-- ============================================================================
select 'aaa' collate utf8_binary;
select 'aaa' collate utf8_lcase;
select 'aaa' collate unicode;
select 'aaa' collate unicode_ci;

-- collation name is case insensitive
select 'aaa' collate uTf8_BiNaRy;
select 'aaa' collate uNicOde;
select 'aaa' collate UNICODE_ci;
select 'aaa' collate UtF8_lCaSE_rtRIM;

-- ============================================================================
-- Collation expression returns name of collation
-- ============================================================================
select collation('aaa' collate utf8_binary);
select collation('aaa' collate utf8_lcase);
select collation('aaa' collate unicode);
select collation('aaa' collate unicode_ci);
select collation('aaa' collate unicode_ci_rtrim);
select collation('aaa' collate utf8_lcase_rtrim);
select collation('aaa' collate utf8_binary_rtrim);

-- collation expression returns default collation
select collation('aaa');

-- ============================================================================
-- Collate function syntax
-- ============================================================================
select collate('aaa', 'utf8_binary');
select collate('aaa', 'utf8_lcase');
select collate('aaa', 'utf8_binary_rtrim');
select collate('aaa', 'utf8_lcase_rtrim');

-- ============================================================================
-- Equality check respects collation
-- ============================================================================
select 'aaa' collate utf8_binary = 'AAA' collate utf8_binary;
select 'aaa' collate utf8_binary = 'aaa' collate utf8_binary;
select 'aaa' collate utf8_binary_rtrim = 'AAA' collate utf8_binary_rtrim;
select 'aaa' collate utf8_binary_rtrim = 'aaa  ' collate utf8_binary_rtrim;
select 'aaa' collate utf8_lcase = 'aaa' collate utf8_lcase;
select 'aaa' collate utf8_lcase = 'AAA' collate utf8_lcase;
select 'aaa' collate utf8_lcase = 'bbb' collate utf8_lcase;
select 'aaa' collate utf8_lcase_rtrim = 'AAA  ' collate utf8_lcase_rtrim;
select 'aaa' collate utf8_lcase_rtrim = 'bbb' collate utf8_lcase_rtrim;
select 'aaa' collate unicode = 'aaa' collate unicode;
select 'aaa' collate unicode = 'AAA' collate unicode;
select 'aaa  ' collate unicode_rtrim = 'aaa ' collate unicode_rtrim;
select 'aaa' collate unicode_rtrim = 'AAA' collate unicode_rtrim;
select 'aaa' collate unicode_CI = 'aaa' collate unicode_CI;
select 'aaa' collate unicode_CI = 'AAA' collate unicode_CI;
select 'aaa' collate unicode_CI = 'bbb' collate unicode_CI;
select 'aaa' collate unicode_CI_rtrim = 'aaa' collate unicode_CI_rtrim;
select 'aaa ' collate unicode_CI_rtrim = 'AAA  ' collate unicode_CI_rtrim;
select 'aaa' collate unicode_CI_rtrim = 'bbb' collate unicode_CI_rtrim;
-- equality with collate function syntax
select collate('aaa', 'utf8_binary') = collate('AAA', 'utf8_binary');
select collate('aaa', 'utf8_lcase') = collate('AAA', 'utf8_lcase');
select collate('aaa', 'unicode_CI') = collate('bbb', 'unicode_CI');

-- ============================================================================
-- Comparisons respect collation
-- ============================================================================
select 'AAA' collate utf8_binary < 'aaa' collate utf8_binary;
select 'aaa' collate utf8_binary < 'aaa' collate utf8_binary;
select 'aaa' collate utf8_binary < 'BBB' collate utf8_binary;
select 'aaa ' collate utf8_binary_rtrim < 'aaa  ' collate utf8_binary_rtrim;
select 'aaa' collate utf8_lcase < 'aaa' collate utf8_lcase;
select 'AAA' collate utf8_lcase < 'aaa' collate utf8_lcase;
select 'aaa' collate utf8_lcase < 'bbb' collate utf8_lcase;
select 'AAA  ' collate utf8_lcase_rtrim < 'aaa' collate utf8_lcase_rtrim;
select 'aaa' collate unicode < 'aaa' collate unicode;
select 'aaa' collate unicode < 'AAA' collate unicode;
select 'aaa' collate unicode < 'BBB' collate unicode;
select 'aaa ' collate unicode_rtrim < 'aaa' collate unicode_rtrim;
select 'aaa' collate unicode_CI < 'aaa' collate unicode_CI;
select 'aaa' collate unicode_CI < 'AAA' collate unicode_CI;
select 'aaa' collate unicode_CI < 'bbb' collate unicode_CI;
select 'aaa ' collate unicode_CI_rtrim < 'aaa' collate unicode_CI_rtrim;
-- comparisons with collate function syntax
select collate('AAA', 'utf8_binary') < collate('aaa', 'utf8_binary');
select collate('aaa', 'utf8_lcase') < collate('bbb', 'utf8_lcase');

-- ============================================================================
-- Aggregates count respects collation
-- ============================================================================
select count(*), c from (select collate(col1, 'utf8_binary') as c from values ('AAA'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'utf8_binary') as c from values ('aaa'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'utf8_binary') as c from values ('aaa'), ('bbb')) t group by c order by c;
select count(*), c from (select collate(col1, 'utf8_binary_rtrim') as c from values ('aaa'), ('aaa ')) t group by c order by c;
select count(*), c from (select collate(col1, 'utf8_lcase') as c from values ('aaa'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'utf8_lcase') as c from values ('AAA'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'utf8_lcase') as c from values ('aaa'), ('bbb')) t group by c order by c;
select count(*), c from (select collate(col1, 'utf8_lcase_rtrim') as c from values ('aaa'), ('AAA  ')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode') as c from values ('AAA'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode') as c from values ('aaa'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode') as c from values ('aaa'), ('bbb')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode_rtrim') as c from values ('aaa'), ('aaa ')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode_CI') as c from values ('aaa'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode_CI') as c from values ('AAA'), ('aaa')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode_CI') as c from values ('aaa'), ('bbb')) t group by c order by c;
select count(*), c from (select collate(col1, 'unicode_CI_rtrim') as c from values ('aaa'), ('AAA ')) t group by c order by c;

-- ============================================================================
-- Cast expressions for collations
-- ============================================================================
SELECT collation(cast('a' as string collate utf8_lcase));
SELECT collation('a' :: string collate utf8_lcase);
SELECT cast(1 as string);
SELECT cast('A' as string);

-- ============================================================================
-- Operations on complex types containing collated strings
-- ============================================================================
select reverse('abc' collate utf8_lcase);
select reverse(array('a' collate utf8_lcase, 'b' collate utf8_lcase));
select array_join(array('a' collate utf8_lcase, 'b' collate utf8_lcase), ', ' collate utf8_lcase);
select array_join(array('a' collate utf8_lcase, 'b' collate utf8_lcase, null), ', ' collate utf8_lcase, 'c' collate utf8_lcase);
select concat('a' collate utf8_lcase, 'b' collate utf8_lcase);
select concat(array('a' collate utf8_lcase, 'b' collate utf8_lcase));
select map('a' collate utf8_lcase, 1, 'b' collate utf8_lcase, 2)['A' collate utf8_lcase];
select map('a' collate utf8_lcase, 1, 'b' collate utf8_lcase, 2)['A'];

-- ============================================================================
-- SR_AI vs SR_Latn_AI collation differences
-- ============================================================================
-- scalastyle:off nonascii
SELECT 'c' = 'ć' COLLATE SR_Latn_AI;
SELECT 'c' = 'ć' COLLATE SR_AI;
SELECT 'ć' = 'č' COLLATE SR_Latn_AI;
SELECT 'ć' = 'č' COLLATE SR_AI;
SELECT 'C' = 'Ć' COLLATE SR_Latn_AI;
SELECT 'C' = 'Ć' COLLATE SR_AI;
SELECT 's' = 'š' COLLATE SR_Latn_AI;
SELECT 's' = 'š' COLLATE SR_AI;
SELECT 'z' = 'ž' COLLATE SR_Latn_AI;
SELECT 'z' = 'ž' COLLATE SR_AI;
-- scalastyle:on nonascii

-- ============================================================================
-- Fully qualified collation names
-- ============================================================================
SELECT collation('a' collate system.builtin.UTF8_BINARY);
SELECT collation('a' collate system.builtin.UTF8_LCASE);
SELECT collation('a' collate system.builtin.UNICODE);
SELECT collation('a' collate system.builtin.UNICODE_CI_AI);
SELECT 'a' collate sYstEm.bUiltIn.utf8_lCAse = 'A';
SELECT contains('a' collate system.builtin.UTF8_LCASE, 'A' collate UTF8_LCASE);
SELECT startswith('a' collate system.builtin.UNICODE_CI, 'A' collate UNICODE_CI);
SELECT endswith('abc' collate system.builtin.UNICODE_CI, 'C' collate UNICODE_CI);

-- ============================================================================
-- ArrayAppend and CreateMap coercion
-- ============================================================================
SELECT array_append(array('a', 'b'), 'c' COLLATE UNICODE);
SELECT typeof(array('a' COLLATE UNICODE, 'b')[1]);
select map('a' COLLATE UTF8_LCASE, 'b', 'c', 'c');

-- ============================================================================
-- Error: invalid collation name
-- ============================================================================
select 'aaa' collate UTF8_BS;

-- ============================================================================
-- Error: collate function syntax invalid arg count
-- ============================================================================
select collate('aaa', 'a', 'b');
select collate('aaa');
select collate();

-- ============================================================================
-- Error: collate function invalid collation data type
-- ============================================================================
select collate('abc', 123);

-- ============================================================================
-- Error: NULL as collation name
-- ============================================================================
select collate('abc', cast(null as string));

-- ============================================================================
-- Error: collate function invalid input data type
-- ============================================================================
select collate(1, 'UTF8_BINARY');

-- ============================================================================
-- Error: collation mismatch for string functions
-- ============================================================================
SELECT contains(collate('abc', 'UNICODE_CI'), collate('b', 'UNICODE'));
SELECT startsWith(collate('abc', 'UNICODE_CI'), collate('a', 'UNICODE'));
SELECT endsWith(collate('abc', 'UNICODE_CI'), collate('c', 'UNICODE'));

-- ============================================================================
-- Error: explicit collation mismatch
-- ============================================================================
SELECT COLLATE('a', 'UTF8_BINARY') = COLLATE('a', 'UNICODE');

-- ============================================================================
-- Error: array collation mismatch
-- ============================================================================
SELECT array('A', 'a' COLLATE UNICODE) == array('b' COLLATE UNICODE_CI);
SELECT array('A', 'a' COLLATE UNICODE) == array('b' COLLATE UNICODE_CI_RTRIM);
SELECT array_join(array('a', 'b' collate UNICODE), 'c' collate UNICODE_CI);

-- ============================================================================
-- Error: invalid fully qualified collation names
-- ============================================================================
SELECT 'a' COLLATE system.builtin2.UTF8_BINARY;
SELECT 'a' COLLATE system.UTF8_BINARY;
SELECT 'a' COLLATE builtin.UTF8_LCASE;

-- ============================================================================
-- Error: conflicting collations in inline table
-- ============================================================================
SELECT * FROM VALUES ('a' COLLATE UTF8_LCASE), ('b' COLLATE UNICODE) AS T(c1);

-- ============================================================================
-- Error: map creation with collation mismatch
-- ============================================================================
select map('a' COLLATE UTF8_LCASE, 'b', 'c' COLLATE UNICODE, 'c');

-- ============================================================================
-- Error: map creation with wrong number of args
-- ============================================================================
select map('a' COLLATE UTF8_LCASE, 'b', 'c');
