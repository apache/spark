-- test cases for string functions with collations

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

create table t2(ascii long) using parquet;
insert into t2 values (97);
insert into t2 values (66);

create table t3(format string collate utf8_binary, utf8_binary string collate utf8_binary, utf8_lcase string collate utf8_lcase) using parquet;
insert into t3 values ('%s%s', 'abCdE', 'abCdE');

-- ConcatWs
select concat_ws(' ', utf8_lcase, utf8_lcase) from t1;
select concat_ws(' ', utf8_binary, utf8_lcase) from t1;
select concat_ws(' ' collate utf8_binary, utf8_binary, 'SQL' collate utf8_lcase) from t1;
select concat_ws(' ' collate utf8_lcase, utf8_binary, 'SQL' collate utf8_lcase) from t1;
select concat_ws(',', utf8_lcase, 'word'), concat_ws(',', utf8_binary, 'word') from t1;
select concat_ws(',', utf8_lcase, 'word' collate utf8_binary), concat_ws(',', utf8_binary, 'word' collate utf8_lcase) from t1;

-- SplitPart
select split_part(utf8_binary, utf8_lcase, 3) from t1;
select split_part(s, utf8_binary, 1) from t1;
select split_part(utf8_binary collate utf8_binary, s collate utf8_lcase, 1) from t1;
select split_part(utf8_binary, utf8_lcase collate utf8_binary, 2) from t1;
select split_part(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 2) from t1;
select split_part(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 2) from t1;
select split_part(utf8_binary, 'a', 3), split_part(utf8_lcase, 'a', 3) from t1;
select split_part(utf8_binary, 'a' collate utf8_lcase, 3), split_part(utf8_lcase, 'a' collate utf8_binary, 3) from t1;
select split_part(utf8_binary, 'a ' collate utf8_lcase_rtrim, 3), split_part(utf8_lcase, 'a' collate utf8_binary, 3) from t1;

-- Contains
select contains(utf8_binary, utf8_lcase) from t1;
select contains(s, utf8_binary) from t1;
select contains(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select contains(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select contains(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select contains(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select contains(utf8_binary, 'a'), contains(utf8_lcase, 'a') from t1;
select contains(utf8_binary, 'AaAA' collate utf8_lcase), contains(utf8_lcase, 'AAa' collate utf8_binary) from t1;
select contains(utf8_binary, 'AaAA ' collate utf8_lcase_rtrim), contains(utf8_lcase, 'AAa ' collate utf8_binary_rtrim) from t1;

-- SubstringIndex
select substring_index(utf8_binary, utf8_lcase, 2) from t1;
select substring_index(s, utf8_binary,1) from t1;
select substring_index(utf8_binary collate utf8_binary, s collate utf8_lcase, 3) from t1;
select substring_index(utf8_binary, utf8_lcase collate utf8_binary, 2) from t1;
select substring_index(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 2) from t1;
select substring_index(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 2) from t1;
select substring_index(utf8_binary, 'a', 2), substring_index(utf8_lcase, 'a', 2) from t1;
select substring_index(utf8_binary, 'AaAA' collate utf8_lcase, 2), substring_index(utf8_lcase, 'AAa' collate utf8_binary, 2) from t1;
select substring_index(utf8_binary, 'AaAA ' collate utf8_lcase_rtrim, 2), substring_index(utf8_lcase, 'AAa' collate utf8_binary, 2) from t1;

-- StringInStr
select instr(utf8_binary, utf8_lcase) from t1;
select instr(s, utf8_binary) from t1;
select instr(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select instr(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select instr(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select instr(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select instr(utf8_binary, 'a'), instr(utf8_lcase, 'a') from t1;
select instr(utf8_binary, 'AaAA' collate utf8_lcase), instr(utf8_lcase, 'AAa' collate utf8_binary) from t1;

-- FindInSet
select find_in_set(utf8_binary, utf8_lcase) from t1;
select find_in_set(s, utf8_binary) from t1;
select find_in_set(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select find_in_set(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select find_in_set(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select find_in_set(utf8_binary, 'aaAaaAaA,i̇o'), find_in_set(utf8_lcase, 'aaAaaAaA,i̇o') from t1;
select find_in_set(utf8_binary, 'aaAaaAaA,i̇o' collate utf8_lcase), find_in_set(utf8_lcase, 'aaAaaAaA,i̇o' collate utf8_binary) from t1;
select find_in_set(utf8_binary, 'aaAaaAaA,i̇o ' collate utf8_lcase_rtrim), find_in_set(utf8_lcase, 'aaAaaAaA,i̇o' collate utf8_binary) from t1;
-- StartsWith
select startswith(utf8_binary, utf8_lcase) from t1;
select startswith(s, utf8_binary) from t1;
select startswith(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select startswith(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select startswith(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select startswith(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select startswith(utf8_binary, 'aaAaaAaA'), startswith(utf8_lcase, 'aaAaaAaA') from t1;
select startswith(utf8_binary, 'aaAaaAaA' collate utf8_lcase), startswith(utf8_lcase, 'aaAaaAaA' collate utf8_binary) from t1;
select startswith(utf8_binary, 'aaAaaAaA ' collate utf8_lcase_rtrim), startswith(utf8_lcase, 'aaAaaAaA' collate utf8_binary) from t1;

-- StringTranslate
select translate(utf8_lcase, utf8_lcase, '12345') from t1;
select translate(utf8_binary, utf8_lcase, '12345') from t1;
select translate(utf8_binary, 'aBc' collate utf8_lcase, '12345' collate utf8_binary) from t1;
select translate(utf8_binary, 'SQL' collate utf8_lcase, '12345' collate utf8_lcase) from t1;
select translate(utf8_binary, 'SQL' collate unicode_ai, '12345' collate unicode_ai) from t1;
select translate(utf8_lcase, 'aaAaaAaA', '12345'), translate(utf8_binary, 'aaAaaAaA', '12345') from t1;
select translate(utf8_lcase, 'aBc' collate utf8_binary, '12345'), translate(utf8_binary, 'aBc' collate utf8_lcase, '12345') from t1;
select translate(utf8_lcase, 'aBc ' collate utf8_binary_rtrim, '12345'), translate(utf8_binary, 'aBc' collate utf8_lcase, '12345') from t1;

-- Replace
select replace(utf8_binary, utf8_lcase, 'abc') from t1;
select replace(s, utf8_binary, 'abc') from t1;
select replace(utf8_binary collate utf8_binary, s collate utf8_lcase, 'abc') from t1;
select replace(utf8_binary, utf8_lcase collate utf8_binary, 'abc') from t1;
select replace(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 'abc') from t1;
select replace(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 'abc') from t1;
select replace(utf8_binary, 'aaAaaAaA', 'abc'), replace(utf8_lcase, 'aaAaaAaA', 'abc') from t1;
select replace(utf8_binary, 'aaAaaAaA' collate utf8_lcase, 'abc'), replace(utf8_lcase, 'aaAaaAaA' collate utf8_binary, 'abc') from t1;
select replace(utf8_binary, 'aaAaaAaA ' collate utf8_lcase_rtrim, 'abc'), replace(utf8_lcase, 'aaAaaAaA' collate utf8_binary, 'abc') from t1;

-- EndsWith
select endswith(utf8_binary, utf8_lcase) from t1;
select endswith(s, utf8_binary) from t1;
select endswith(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select endswith(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select endswith(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select endswith(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select endswith(utf8_binary, 'aaAaaAaA'), endswith(utf8_lcase, 'aaAaaAaA') from t1;
select endswith(utf8_binary, 'aaAaaAaA' collate utf8_lcase), endswith(utf8_lcase, 'aaAaaAaA' collate utf8_binary) from t1;
select endswith(utf8_binary, 'aaAaaAaA ' collate utf8_lcase_rtrim), endswith(utf8_lcase, 'aaAaaAaA' collate utf8_binary) from t1;

-- StringRepeat
select repeat(utf8_binary, 3), repeat(utf8_lcase, 2) from t1;
select repeat(utf8_binary collate utf8_lcase, 3), repeat(utf8_lcase collate utf8_binary, 2) from t1;

-- Chr
select chr(ascii) from t2;

-- Sentences
select sentences(utf8_binary), sentences(utf8_lcase) from t1;
select sentences(utf8_binary collate utf8_lcase), sentences(utf8_lcase collate utf8_binary) from t1;

-- Upper
select upper(utf8_binary), upper(utf8_lcase) from t1;
select upper(utf8_binary collate utf8_lcase), upper(utf8_lcase collate utf8_binary) from t1;

-- Lower
select lower(utf8_binary), lower(utf8_lcase) from t1;
select lower(utf8_binary collate utf8_lcase), lower(utf8_lcase collate utf8_binary) from t1;

-- InitCap
select initcap(utf8_binary), initcap(utf8_lcase) from t1;
select initcap(utf8_binary collate utf8_lcase), initcap(utf8_lcase collate utf8_binary) from t1;

-- Overlay
select overlay(utf8_binary, utf8_lcase, 2) from t1;
select overlay(s, utf8_binary,1) from t1;
select overlay(utf8_binary collate utf8_binary, s collate utf8_lcase, 3) from t1;
select overlay(utf8_binary, utf8_lcase collate utf8_binary, 2) from t1;
select overlay(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 2) from t1;
select overlay(utf8_binary, 'a', 2), overlay(utf8_lcase, 'a', 2) from t1;
select overlay(utf8_binary, 'AaAA' collate utf8_lcase, 2), overlay(utf8_lcase, 'AAa' collate utf8_binary, 2) from t1;

-- FormatString
select format_string(format, utf8_binary, utf8_lcase) from t3;
select format_string(format collate utf8_lcase, utf8_lcase, utf8_binary collate utf8_lcase, 3), format_string(format, utf8_lcase collate utf8_binary, utf8_binary) from t3;
select format_string(format, utf8_binary, utf8_lcase) from t3;

-- Length, BitLength & OctetLength
select length(utf8_binary), length(utf8_lcase) from t1;
select length(utf8_binary collate utf8_lcase), length(utf8_lcase collate utf8_binary) from t1;
select bit_length(utf8_binary), bit_length(utf8_lcase) from t1;
select bit_length(utf8_binary collate utf8_lcase), bit_length(utf8_lcase collate utf8_binary) from t1;
select octet_length(utf8_binary), octet_length(utf8_lcase) from t1;
select octet_length(utf8_binary collate utf8_lcase), octet_length(utf8_lcase collate utf8_binary) from t1;
select octet_length(utf8_binary collate utf8_lcase_rtrim), octet_length(utf8_lcase collate utf8_binary_rtrim) from t1;

-- Left/Right/Substr
select substr(utf8_binary, 2, 2), substr(utf8_lcase, 2, 2) from t1;
select substr(utf8_binary collate utf8_lcase, 2, 2), substr(utf8_lcase collate utf8_binary, 2, 2) from t1;
select right(utf8_binary, 2), right(utf8_lcase, 2) from t1;
select right(utf8_binary collate utf8_lcase, 2), right(utf8_lcase collate utf8_binary, 2) from t1;
select left(utf8_binary, '2' collate utf8_lcase), left(utf8_lcase, 2) from t1;
select left(utf8_binary collate utf8_lcase, 2), left(utf8_lcase collate utf8_binary, 2) from t1;

-- Locate
select locate(utf8_binary, utf8_lcase) from t1;
select locate(s, utf8_binary) from t1;
select locate(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select locate(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select locate(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase, 3) from t1;
select locate(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai, 3) from t1;
select locate(utf8_binary, 'a'), locate(utf8_lcase, 'a') from t1;
select locate(utf8_binary, 'AaAA' collate utf8_lcase, 4), locate(utf8_lcase, 'AAa' collate utf8_binary, 4) from t1;
select locate(utf8_binary, 'AaAA ' collate utf8_binary_rtrim, 4), locate(utf8_lcase, 'AAa ' collate utf8_binary, 4) from t1;

drop table t1;
drop table t2;
drop table t3;
