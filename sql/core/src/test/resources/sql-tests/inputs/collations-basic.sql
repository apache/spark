-- test cases for collation support

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
