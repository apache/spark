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
create table t4 (text string collate utf8_binary, pairDelim string collate utf8_lcase, keyValueDelim string collate utf8_binary) using parquet;

insert into t4 values('a:1,b:2,c:3', ',', ':');

select str_to_map(text, pairDelim, keyValueDelim) from t4;
select str_to_map(text collate utf8_binary, pairDelim collate utf8_lcase, keyValueDelim collate utf8_binary) from t4;
select str_to_map(text collate utf8_binary, pairDelim collate utf8_binary, keyValueDelim collate utf8_binary) from t4;
select str_to_map(text collate unicode_ai, pairDelim collate unicode_ai, keyValueDelim collate unicode_ai) from t4;

drop table t4;

create table t5(s string, utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t5 values ('Spark', 'Spark', 'SQL');
insert into t5 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaAAaA');
insert into t5 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaaAaA');
insert into t5 values ('aaAaAAaA', 'aaAaAAaA', 'aaAaaAaAaaAaaAaAaaAaaAaA');
insert into t5 values ('bbAbaAbA', 'bbAbAAbA', 'a');
insert into t5 values ('İo', 'İo', 'İo');
insert into t5 values ('İo', 'İo', 'i̇o');
insert into t5 values ('efd2', 'efd2', 'efd2');
insert into t5 values ('Hello, world! Nice day.', 'Hello, world! Nice day.', 'Hello, world! Nice day.');
insert into t5 values ('Something else. Nothing here.', 'Something else. Nothing here.', 'Something else. Nothing here.');
insert into t5 values ('kitten', 'kitten', 'sitTing');
insert into t5 values ('abc', 'abc', 'abc');
insert into t5 values ('abcdcba', 'abcdcba', 'aBcDCbA');

create table t6(ascii long) using parquet;
insert into t6 values (97);
insert into t6 values (66);

create table t7(ascii double) using parquet;
insert into t7 values (97.52143);
insert into t7 values (66.421);

create table t8(format string collate utf8_binary, utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t8 values ('%s%s', 'abCdE', 'abCdE');

create table t9(num long) using parquet;
insert into t9 values (97);
insert into t9 values (66);

create table t10(utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t10 values ('aaAaAAaA', 'aaAaaAaA');
insert into t10 values ('efd2', 'efd2');

-- ConcatWs
select concat_ws(' ', utf8_lcase, utf8_lcase) from t5;
select concat_ws(' ', utf8_binary, utf8_lcase) from t5;
select concat_ws(' ' collate utf8_binary, utf8_binary, 'SQL' collate utf8_lcase) from t5;
select concat_ws(' ' collate utf8_lcase, utf8_binary, 'SQL' collate utf8_lcase) from t5;
select concat_ws(',', utf8_lcase, 'word'), concat_ws(',', utf8_binary, 'word') from t5;
select concat_ws(',', utf8_lcase, 'word' collate utf8_binary), concat_ws(',', utf8_binary, 'word' collate utf8_lcase) from t5;

-- Elt
select elt(2, s, utf8_binary) from t5;
select elt(2, utf8_binary, utf8_lcase, s) from t5;
select elt(1, utf8_binary collate utf8_binary, utf8_lcase collate utf8_lcase) from t5;
select elt(1, utf8_binary collate utf8_binary, utf8_lcase collate utf8_binary) from t5;
select elt(1, utf8_binary collate utf8_binary, utf8_lcase) from t5;
select elt(1, utf8_binary, 'word'), elt(1, utf8_lcase, 'word') from t5;
select elt(1, utf8_binary, 'word' collate utf8_lcase), elt(1, utf8_lcase, 'word' collate utf8_binary) from t5;

-- SplitPart
select split_part(utf8_binary, utf8_lcase, 3) from t5;
select split_part(s, utf8_binary, 1) from t5;
select split_part(utf8_binary collate utf8_binary, s collate utf8_lcase, 1) from t5;
select split_part(utf8_binary, utf8_lcase collate utf8_binary, 2) from t5;
select split_part(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 2) from t5;
select split_part(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 2) from t5;
select split_part(utf8_binary, 'a', 3), split_part(utf8_lcase, 'a', 3) from t5;
select split_part(utf8_binary, 'a' collate utf8_lcase, 3), split_part(utf8_lcase, 'a' collate utf8_binary, 3) from t5;

-- Contains
select contains(utf8_binary, utf8_lcase) from t5;
select contains(s, utf8_binary) from t5;
select contains(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select contains(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select contains(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select contains(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select contains(utf8_binary, 'a'), contains(utf8_lcase, 'a') from t5;
select contains(utf8_binary, 'AaAA' collate utf8_lcase), contains(utf8_lcase, 'AAa' collate utf8_binary) from t5;

-- SubstringIndex
select substring_index(utf8_binary, utf8_lcase, 2) from t5;
select substring_index(s, utf8_binary,1) from t5;
select substring_index(utf8_binary collate utf8_binary, s collate utf8_lcase, 3) from t5;
select substring_index(utf8_binary, utf8_lcase collate utf8_binary, 2) from t5;
select substring_index(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 2) from t5;
select substring_index(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 2) from t5;
select substring_index(utf8_binary, 'a', 2), substring_index(utf8_lcase, 'a', 2) from t5;
select substring_index(utf8_binary, 'AaAA' collate utf8_lcase, 2), substring_index(utf8_lcase, 'AAa' collate utf8_binary, 2) from t5;

-- StringInStr
select instr(utf8_binary, utf8_lcase) from t5;
select instr(s, utf8_binary) from t5;
select instr(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select instr(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select instr(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select instr(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select instr(utf8_binary, 'a'), instr(utf8_lcase, 'a') from t5;
select instr(utf8_binary, 'AaAA' collate utf8_lcase), instr(utf8_lcase, 'AAa' collate utf8_binary) from t5;

-- FindInSet
select find_in_set(utf8_binary, utf8_lcase) from t5;
select find_in_set(s, utf8_binary) from t5;
select find_in_set(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select find_in_set(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select find_in_set(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select find_in_set(utf8_binary, 'aaAaaAaA,i̇o'), find_in_set(utf8_lcase, 'aaAaaAaA,i̇o') from t5;
select find_in_set(utf8_binary, 'aaAaaAaA,i̇o' collate utf8_lcase), find_in_set(utf8_lcase, 'aaAaaAaA,i̇o' collate utf8_binary) from t5;

-- StartsWith
select startswith(utf8_binary, utf8_lcase) from t5;
select startswith(s, utf8_binary) from t5;
select startswith(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select startswith(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select startswith(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select startswith(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select startswith(utf8_binary, 'aaAaaAaA'), startswith(utf8_lcase, 'aaAaaAaA') from t5;
select startswith(utf8_binary, 'aaAaaAaA' collate utf8_lcase), startswith(utf8_lcase, 'aaAaaAaA' collate utf8_binary) from t5;

-- StringTranslate
select translate(utf8_lcase, utf8_lcase, '12345') from t5;
select translate(utf8_binary, utf8_lcase, '12345') from t5;
select translate(utf8_binary, 'aBc' collate utf8_lcase, '12345' collate utf8_binary) from t5;
select translate(utf8_binary, 'SQL' collate utf8_lcase, '12345' collate utf8_lcase) from t5;
select translate(utf8_binary, 'SQL' collate unicode_ai, '12345' collate unicode_ai) from t5;
select translate(utf8_lcase, 'aaAaaAaA', '12345'), translate(utf8_binary, 'aaAaaAaA', '12345') from t5;
select translate(utf8_lcase, 'aBc' collate utf8_binary, '12345'), translate(utf8_binary, 'aBc' collate utf8_lcase, '12345') from t5;

-- Replace
select replace(utf8_binary, utf8_lcase, 'abc') from t5;
select replace(s, utf8_binary, 'abc') from t5;
select replace(utf8_binary collate utf8_binary, s collate utf8_lcase, 'abc') from t5;
select replace(utf8_binary, utf8_lcase collate utf8_binary, 'abc') from t5;
select replace(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 'abc') from t5;
select replace(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 'abc') from t5;
select replace(utf8_binary, 'aaAaaAaA', 'abc'), replace(utf8_lcase, 'aaAaaAaA', 'abc') from t5;
select replace(utf8_binary, 'aaAaaAaA' collate utf8_lcase, 'abc'), replace(utf8_lcase, 'aaAaaAaA' collate utf8_binary, 'abc') from t5;

-- EndsWith
select endswith(utf8_binary, utf8_lcase) from t5;
select endswith(s, utf8_binary) from t5;
select endswith(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select endswith(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select endswith(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select endswith(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select endswith(utf8_binary, 'aaAaaAaA'), endswith(utf8_lcase, 'aaAaaAaA') from t5;
select endswith(utf8_binary, 'aaAaaAaA' collate utf8_lcase), endswith(utf8_lcase, 'aaAaaAaA' collate utf8_binary) from t5;

-- StringRepeat
select repeat(utf8_binary, 3), repeat(utf8_lcase, 2) from t5;
select repeat(utf8_binary collate utf8_lcase, 3), repeat(utf8_lcase collate utf8_binary, 2) from t5;

-- Ascii & UnBase64 string expressions
select ascii(utf8_binary), ascii(utf8_lcase) from t5;
select ascii(utf8_binary collate utf8_lcase), ascii(utf8_lcase collate utf8_binary) from t5;
select unbase64(utf8_binary), unbase64(utf8_lcase) from t10;
select unbase64(utf8_binary collate utf8_lcase), unbase64(utf8_lcase collate utf8_binary) from t10;

-- Chr
select chr(ascii) from t6;

-- Base64, Decode
select base64(utf8_binary), base64(utf8_lcase) from t5;
select base64(utf8_binary collate utf8_lcase), base64(utf8_lcase collate utf8_binary) from t5;
select decode(encode(utf8_binary, 'utf-8'), 'utf-8'), decode(encode(utf8_lcase, 'utf-8'), 'utf-8') from t5;
select decode(encode(utf8_binary collate utf8_lcase, 'utf-8'), 'utf-8'), decode(encode(utf8_lcase collate utf8_binary, 'utf-8'), 'utf-8') from t5;

-- FormatNumber
select format_number(ascii, '###.###') from t7;
select format_number(ascii, '###.###' collate utf8_lcase) from t7;

-- Encode, ToBinary
select encode(utf8_binary, 'utf-8'), encode(utf8_lcase, 'utf-8') from t5;
select encode(utf8_binary collate utf8_lcase, 'utf-8'), encode(utf8_lcase collate utf8_binary, 'utf-8') from t5;
select to_binary(utf8_binary, 'utf-8'), to_binary(utf8_lcase, 'utf-8') from t5;
select to_binary(utf8_binary collate utf8_lcase, 'utf-8'), to_binary(utf8_lcase collate utf8_binary, 'utf-8') from t5;

-- Sentences
select sentences(utf8_binary), sentences(utf8_lcase) from t5;
select sentences(utf8_binary collate utf8_lcase), sentences(utf8_lcase collate utf8_binary) from t5;

-- Upper
select upper(utf8_binary), upper(utf8_lcase) from t5;
select upper(utf8_binary collate utf8_lcase), upper(utf8_lcase collate utf8_binary) from t5;

-- Lower
select lower(utf8_binary), lower(utf8_lcase) from t5;
select lower(utf8_binary collate utf8_lcase), lower(utf8_lcase collate utf8_binary) from t5;

-- InitCap
select initcap(utf8_binary), initcap(utf8_lcase) from t5;
select initcap(utf8_binary collate utf8_lcase), initcap(utf8_lcase collate utf8_binary) from t5;

-- Overlay
select overlay(utf8_binary, utf8_lcase, 2) from t5;
select overlay(s, utf8_binary,1) from t5;
select overlay(utf8_binary collate utf8_binary, s collate utf8_lcase, 3) from t5;
select overlay(utf8_binary, utf8_lcase collate utf8_binary, 2) from t5;
select overlay(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 2) from t5;
select overlay(utf8_binary, 'a', 2), overlay(utf8_lcase, 'a', 2) from t5;
select overlay(utf8_binary, 'AaAA' collate utf8_lcase, 2), overlay(utf8_lcase, 'AAa' collate utf8_binary, 2) from t5;

-- FormatString
select format_string(format, utf8_binary, utf8_lcase) from t8;
select format_string(format collate utf8_lcase, utf8_lcase, utf8_binary collate utf8_lcase, 3), format_string(format, utf8_lcase collate utf8_binary, utf8_binary) from t8;
select format_string(format, utf8_binary, utf8_lcase) from t8;

-- SoundEx
select soundex(utf8_binary), soundex(utf8_lcase) from t5;
select soundex(utf8_binary collate utf8_lcase), soundex(utf8_lcase collate utf8_binary) from t5;

-- Length, BitLength & OctetLength
select length(utf8_binary), length(utf8_lcase) from t5;
select length(utf8_binary collate utf8_lcase), length(utf8_lcase collate utf8_binary) from t5;
select bit_length(utf8_binary), bit_length(utf8_lcase) from t5;
select bit_length(utf8_binary collate utf8_lcase), bit_length(utf8_lcase collate utf8_binary) from t5;
select octet_length(utf8_binary), octet_length(utf8_lcase) from t5;
select octet_length(utf8_binary collate utf8_lcase), octet_length(utf8_lcase collate utf8_binary) from t5;

-- Luhncheck
select luhn_check(num) from t9;

-- Levenshtein
select levenshtein(utf8_binary, utf8_lcase) from t5;
select levenshtein(s, utf8_binary) from t5;
select levenshtein(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select levenshtein(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select levenshtein(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select levenshtein(utf8_binary, 'a'), levenshtein(utf8_lcase, 'a') from t5;
select levenshtein(utf8_binary, 'AaAA' collate utf8_lcase, 3), levenshtein(utf8_lcase, 'AAa' collate utf8_binary, 4) from t5;

-- IsValidUTF8
select is_valid_utf8(utf8_binary), is_valid_utf8(utf8_lcase) from t5;
select is_valid_utf8(utf8_binary collate utf8_lcase), is_valid_utf8(utf8_lcase collate utf8_binary) from t5;

-- MakeValidUTF8
select make_valid_utf8(utf8_binary), make_valid_utf8(utf8_lcase) from t5;
select make_valid_utf8(utf8_binary collate utf8_lcase), make_valid_utf8(utf8_lcase collate utf8_binary) from t5;

-- ValidateUTF8
select validate_utf8(utf8_binary), validate_utf8(utf8_lcase) from t5;
select validate_utf8(utf8_binary collate utf8_lcase), validate_utf8(utf8_lcase collate utf8_binary) from t5;

-- TryValidateUTF8
select try_validate_utf8(utf8_binary), try_validate_utf8(utf8_lcase) from t5;
select try_validate_utf8(utf8_binary collate utf8_lcase), try_validate_utf8(utf8_lcase collate utf8_binary) from t5;

-- Left/Right/Substr
select substr(utf8_binary, 2, 2), substr(utf8_lcase, 2, 2) from t5;
select substr(utf8_binary collate utf8_lcase, 2, 2), substr(utf8_lcase collate utf8_binary, 2, 2) from t5;
select right(utf8_binary, 2), right(utf8_lcase, 2) from t5;
select right(utf8_binary collate utf8_lcase, 2), right(utf8_lcase collate utf8_binary, 2) from t5;
select left(utf8_binary, '2' collate utf8_lcase), left(utf8_lcase, 2) from t5;
select left(utf8_binary collate utf8_lcase, 2), left(utf8_lcase collate utf8_binary, 2) from t5;

-- StringRPad
select rpad(utf8_binary, 8, utf8_lcase) from t5;
select rpad(s, 8, utf8_binary) from t5;
select rpad(utf8_binary collate utf8_binary, 8, s collate utf8_lcase) from t5;
select rpad(utf8_binary, 8, utf8_lcase collate utf8_binary) from t5;
select rpad(utf8_binary collate utf8_lcase, 8, utf8_lcase collate utf8_lcase) from t5;
select rpad(utf8_binary, 8, 'a'), rpad(utf8_lcase, 8, 'a') from t5;
select rpad(utf8_binary, 8, 'AaAA' collate utf8_lcase), rpad(utf8_lcase, 8, 'AAa' collate utf8_binary) from t5;

-- StringLPad
select lpad(utf8_binary, 8, utf8_lcase) from t5;
select lpad(s, 8, utf8_binary) from t5;
select lpad(utf8_binary collate utf8_binary, 8, s collate utf8_lcase) from t5;
select lpad(utf8_binary, 8, utf8_lcase collate utf8_binary) from t5;
select lpad(utf8_binary collate utf8_lcase, 8, utf8_lcase collate utf8_lcase) from t5;
select lpad(utf8_binary, 8, 'a'), lpad(utf8_lcase, 8, 'a') from t5;
select lpad(utf8_binary, 8, 'AaAA' collate utf8_lcase), lpad(utf8_lcase, 8, 'AAa' collate utf8_binary) from t5;

-- Locate
select locate(utf8_binary, utf8_lcase) from t5;
select locate(s, utf8_binary) from t5;
select locate(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select locate(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select locate(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 3) from t5;
select locate(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 3) from t5;
select locate(utf8_binary, 'a'), locate(utf8_lcase, 'a') from t5;
select locate(utf8_binary, 'AaAA' collate utf8_lcase, 4), locate(utf8_lcase, 'AAa' collate utf8_binary, 4) from t5;

-- StringTrim
select TRIM(utf8_binary, utf8_lcase) from t5;
select TRIM(s, utf8_binary) from t5;
select TRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select TRIM(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select TRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select TRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select TRIM('ABc', utf8_binary), TRIM('ABc', utf8_lcase) from t5;
select TRIM('ABc' collate utf8_lcase, utf8_binary), TRIM('AAa' collate utf8_binary, utf8_lcase) from t5;
-- StringTrimBoth
select BTRIM(utf8_binary, utf8_lcase) from t5;
select BTRIM(s, utf8_binary) from t5;
select BTRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select BTRIM(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select BTRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select BTRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select BTRIM('ABc', utf8_binary), BTRIM('ABc', utf8_lcase) from t5;
select BTRIM('ABc' collate utf8_lcase, utf8_binary), BTRIM('AAa' collate utf8_binary, utf8_lcase) from t5;
-- StringTrimLeft
select LTRIM(utf8_binary, utf8_lcase) from t5;
select LTRIM(s, utf8_binary) from t5;
select LTRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select LTRIM(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select LTRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select LTRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select LTRIM('ABc', utf8_binary), LTRIM('ABc', utf8_lcase) from t5;
select LTRIM('ABc' collate utf8_lcase, utf8_binary), LTRIM('AAa' collate utf8_binary, utf8_lcase) from t5;
-- StringTrimRight
select RTRIM(utf8_binary, utf8_lcase) from t5;
select RTRIM(s, utf8_binary) from t5;
select RTRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t5;
select RTRIM(utf8_binary, utf8_lcase collate utf8_binary) from t5;
select RTRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t5;
select RTRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t5;
select RTRIM('ABc', utf8_binary), RTRIM('ABc', utf8_lcase) from t5;
select RTRIM('ABc' collate utf8_lcase, utf8_binary), RTRIM('AAa' collate utf8_binary, utf8_lcase) from t5;

drop table t5;
drop table t6;
drop table t7;
drop table t8;
drop table t9;
drop table t10;
