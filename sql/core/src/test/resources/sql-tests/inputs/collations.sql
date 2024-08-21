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

-- ConcatWs
SELECT concat_ws(collate(' ', 'UTF8_BINARY'), collate('Spark', 'UTF8_BINARY'), collate('SQL', 'UTF8_BINARY'));
SELECT concat_ws(collate(' ', 'UTF8_BINARY'), 'Spark', 'SQL');
SELECT concat_ws(' ', collate('Spark', 'UTF8_BINARY'), collate('SQL', 'UTF8_BINARY'));
SELECT concat_ws(collate(' ', 'UTF8_LCASE'), collate('Spark', 'UTF8_LCASE'), collate('SQL', 'UTF8_LCASE'));
SELECT concat_ws(collate(' ', 'UTF8_LCASE'), 'Spark', 'SQL');
SELECT concat_ws(' ', collate('Spark', 'UTF8_LCASE'), collate('SQL', 'UTF8_LCASE'));
SELECT concat_ws(collate(' ', 'UNICODE'), collate('Spark', 'UNICODE'), collate('SQL', 'UNICODE'));
SELECT concat_ws(collate(' ', 'UNICODE'), 'Spark', 'SQL');
SELECT concat_ws(' ', collate('Spark', 'UNICODE'), collate('SQL', 'UNICODE'));
SELECT concat_ws(collate(' ', 'UNICODE_CI'), collate('Spark', 'UNICODE_CI'), collate('SQL', 'UNICODE_CI'));
SELECT concat_ws(collate(' ', 'UNICODE_CI'), 'Spark', 'SQL');
SELECT concat_ws(' ', collate('Spark', 'UNICODE_CI'), collate('SQL', 'UNICODE_CI'));
SELECT concat_ws(' ', collate('Spark', 'UTF8_LCASE'), collate('SQL', 'UNICODE'));

-- Elt
SELECT elt(1, collate('Spark', 'UTF8_BINARY'), collate('SQL', 'UTF8_BINARY'));
SELECT elt(1, collate('Spark', 'UTF8_BINARY'), 'SQL');
SELECT elt(1, 'Spark', collate('SQL', 'UTF8_BINARY'));
SELECT elt(1, collate('Spark', 'UTF8_LCASE'), collate('SQL', 'UTF8_LCASE'));
SELECT elt(1, collate('Spark', 'UTF8_LCASE'), 'SQL');
SELECT elt(1, 'Spark', collate('SQL', 'UTF8_LCASE'));
SELECT elt(2, collate('Spark', 'UNICODE'), collate('SQL', 'UNICODE'));
SELECT elt(2, collate('Spark', 'UNICODE'), 'SQL');
SELECT elt(2, 'Spark', collate('SQL', 'UNICODE'));
SELECT elt(2, collate('Spark', 'UNICODE_CI'), collate('SQL', 'UNICODE_CI'));
SELECT elt(2, collate('Spark', 'UNICODE_CI'), 'SQL');
SELECT elt(2, 'Spark', collate('SQL', 'UNICODE_CI'));
SELECT elt(0, collate('Spark', 'UTF8_LCASE'), collate('SQL', 'UNICODE'));

-- SplitPart
SELECT split_part(collate('1a2','UTF8_BINARY'),collate('a','UTF8_BINARY'),2);
SELECT split_part(collate('1a2','UNICODE'),collate('a','UNICODE'),2);
SELECT split_part(collate('1a2','UTF8_LCASE'),collate('A','UTF8_LCASE'),2);
SELECT split_part(collate('1a2','UNICODE_CI'),collate('A','UNICODE_CI'),2);

-- Contains
SELECT contains(collate('','UTF8_BINARY'),collate('','UTF8_BINARY'));
SELECT contains(collate('','UTF8_BINARY'),'');
SELECT contains('',collate('','UTF8_BINARY'));
SELECT contains(collate('abcde','UNICODE'),collate('C','UNICODE'));
SELECT contains(collate('abcde','UNICODE'),'C');
SELECT contains('abcde',collate('C','UNICODE'));
SELECT contains(collate('abcde','UTF8_LCASE'),collate('FGH','UTF8_LCASE'));
SELECT contains(collate('abcde','UTF8_LCASE'),'FGH');
SELECT contains('abcde',collate('FGH','UTF8_LCASE'));
SELECT contains(collate('abcde','UNICODE_CI'),collate('BCD','UNICODE_CI'));
SELECT contains(collate('abcde','UNICODE_CI'),'BCD');
SELECT contains('abcde',collate('BCD','UNICODE_CI'));
SELECT contains(collate('abcde', 'UTF8_LCASE'), collate('C', 'UNICODE_CI'));

-- SubstringIndex
SELECT substring_index(collate('wwwgapachegorg','UTF8_BINARY'),collate('g','UTF8_BINARY'),-3);
SELECT substring_index(collate('wwwgapachegorg','UTF8_BINARY'),'g',-3);
SELECT substring_index('wwwgapachegorg',collate('g','UTF8_BINARY'),-3);
SELECT substring_index(collate('www||apache||org','UTF8_BINARY'),collate('||','UTF8_BINARY'),2);
SELECT substring_index(collate('www||apache||org','UTF8_BINARY'),'||',2);
SELECT substring_index('www||apache||org',collate('||','UTF8_BINARY'),2);
SELECT substring_index(collate('wwwXapacheXorg','UTF8_LCASE'),collate('x','UTF8_LCASE'),2);
SELECT substring_index(collate('wwwXapacheXorg','UTF8_LCASE'),'x',2);
SELECT substring_index('wwwXapacheXorg',collate('x','UTF8_LCASE'),2);
SELECT substring_index(collate('aaaaaaaaaa','UNICODE'),collate('aa','UNICODE'),2);
SELECT substring_index(collate('aaaaaaaaaa','UNICODE'),'aa',2);
SELECT substring_index('aaaaaaaaaa',collate('aa','UNICODE'),2);
SELECT substring_index(collate('wwwmapacheMorg','UNICODE_CI'),collate('M','UNICODE_CI'),-2);
SELECT substring_index(collate('wwwmapacheMorg','UNICODE_CI'),'M',-2);
SELECT substring_index('wwwmapacheMorg',collate('M','UNICODE_CI'),-2);
SELECT substring_index(collate('abcde', 'UTF8_LCASE'), collate('C', 'UNICODE_CI'),1);

-- StringInStr
SELECT instr(collate('test大千世界X大千世界','UTF8_BINARY'),collate('大千','UTF8_BINARY'));
SELECT instr(collate('test大千世界X大千世界','UTF8_BINARY'),'大千');
SELECT instr('test大千世界X大千世界',collate('大千','UTF8_BINARY'));
SELECT instr(collate('test大千世界X大千世界','UTF8_LCASE'),collate('界x','UTF8_LCASE'));
SELECT instr(collate('test大千世界X大千世界','UTF8_LCASE'),'界x');
SELECT instr('test大千世界X大千世界',collate('界x','UTF8_LCASE'));
SELECT instr(collate('test大千世界X大千世界','UNICODE'),collate('界x','UNICODE'));
SELECT instr(collate('test大千世界X大千世界','UNICODE'),'界x');
SELECT instr('test大千世界X大千世界',collate('界x','UNICODE'));
SELECT instr(collate('test大千世界X大千世界','UNICODE_CI'),collate('界y','UNICODE_CI'));
SELECT instr(collate('test大千世界X大千世界','UNICODE_CI'),'界y');
SELECT instr('test大千世界X大千世界',collate('界y','UNICODE_CI'));
SELECT instr(collate('test大千世界X大千世界','UNICODE_CI'),collate('界x','UNICODE_CI'));
SELECT instr(collate('test大千世界X大千世界','UNICODE_CI'),'界x');
SELECT instr('test大千世界X大千世界',collate('界x','UNICODE_CI'));
SELECT instr(collate('abİo12','UNICODE_CI'),collate('i̇o','UNICODE_CI'));
SELECT instr(collate('abİo12','UNICODE_CI'),'i̇o');
SELECT instr('abİo12',collate('i̇o','UNICODE_CI'));
SELECT instr(collate('aaads', 'UTF8_BINARY'), collate('Aa', 'UTF8_LCASE'));

-- FindInSet
SELECT find_in_set(collate('AB', 'UTF8_BINARY'),collate('abc,b,ab,c,def', 'UTF8_BINARY'));
SELECT find_in_set(collate('AB', 'UTF8_BINARY'),'abc,b,ab,c,def');
SELECT find_in_set('AB',collate('abc,b,ab,c,def', 'UTF8_BINARY'));
SELECT find_in_set(collate('C', 'UTF8_LCASE'),collate('abc,b,ab,c,def', 'UTF8_LCASE'));
SELECT find_in_set(collate('C', 'UTF8_LCASE'),'abc,b,ab,c,def');
SELECT find_in_set('C',collate('abc,b,ab,c,def', 'UTF8_LCASE'));
SELECT find_in_set(collate('d,ef', 'UNICODE'),collate('abc,b,ab,c,def', 'UNICODE'));
SELECT find_in_set(collate('d,ef', 'UNICODE'),'abc,b,ab,c,def');
SELECT find_in_set('d,ef',collate('abc,b,ab,c,def', 'UNICODE'));
SELECT find_in_set(collate('i̇o', 'UNICODE_CI'),collate('ab,İo,12', 'UNICODE_CI'));
SELECT find_in_set(collate('i̇o', 'UNICODE_CI'),'ab,İo,12');
SELECT find_in_set('i̇o',collate('ab,İo,12', 'UNICODE_CI'));
SELECT find_in_set(collate('İo', 'UNICODE_CI'),collate('ab,i̇o,12', 'UNICODE_CI'));
SELECT find_in_set(collate('İo', 'UNICODE_CI'),'ab,i̇o,12');
SELECT find_in_set('İo',collate('ab,i̇o,12', 'UNICODE_CI'));
SELECT find_in_set(collate('AB', 'UTF8_BINARY'), collate('ab,xyz,fgh', 'UTF8_LCASE'));

-- StartsWith
SELECT startswith(collate('','UTF8_BINARY'),collate('','UTF8_BINARY'));
SELECT startswith(collate('', 'UTF8_BINARY'),'');
SELECT startswith('', collate('', 'UTF8_BINARY'));
SELECT startswith(collate('abcde','UNICODE'),collate('A','UNICODE'));
SELECT startswith(collate('abcde', 'UNICODE'),'A');
SELECT startswith('abcde', collate('A', 'UNICODE'));
SELECT startswith(collate('abcde','UTF8_LCASE'),collate('FGH','UTF8_LCASE'));
SELECT startswith(collate('abcde', 'UTF8_LCASE'),'FGH');
SELECT startswith('abcde', collate('FGH', 'UTF8_LCASE'));
SELECT startswith(collate('abcde','UNICODE_CI'),collate('ABC','UNICODE_CI'));
SELECT startswith(collate('abcde', 'UNICODE_CI'),'ABC');
SELECT startswith('abcde', collate('ABC', 'UNICODE_CI'));
SELECT startswith(collate('abcde', 'UTF8_LCASE'), collate('C', 'UNICODE_CI'));

-- StringTranslate
SELECT translate(collate('Translate', 'UTF8_BINARY'),collate('Rnlt', 'UTF8_BINARY'),collate('12', 'UTF8_BINARY'));
SELECT translate(collate('Translate', 'UTF8_BINARY'),'Rnlt', '12');
SELECT translate('Translate', collate('Rnlt','UTF8_BINARY'), '12');
SELECT translate('Translate', 'Rnlt',collate('12', 'UTF8_BINARY'));
SELECT translate(collate('Translate', 'UTF8_LCASE'),collate('Rnlt', 'UTF8_LCASE'),collate('1234', 'UTF8_LCASE'));
SELECT translate(collate('Translate', 'UTF8_LCASE'),'Rnlt', '1234');
SELECT translate('Translate', collate('Rnlt','UTF8_LCASE'), '1234');
SELECT translate('Translate', 'Rnlt',collate('1234', 'UTF8_LCASE'));
SELECT translate(collate('Translate', 'UNICODE'),collate('Rn', 'UNICODE'),collate('  ', 'UNICODE'));
SELECT translate(collate('Translate', 'UNICODE'),'Rn', '  ');
SELECT translate('Translate', collate('Rn','UNICODE'), '  ');
SELECT translate('Translate', 'Rn',collate('  ', 'UNICODE'));
SELECT translate(collate('Translate', 'UNICODE_CI'),collate('Rn', 'UNICODE_CI'),collate('1234', 'UNICODE_CI'));
SELECT translate(collate('Translate', 'UNICODE_CI'),'Rn', '1234');
SELECT translate('Translate', collate('Rn','UNICODE_CI'), '1234');
SELECT translate('Translate', 'Rn',collate('1234', 'UNICODE_CI'));
SELECT translate(collate('Translate', 'UTF8_LCASE'), collate('Rnlt', 'UNICODE'), '1234');

-- Replace
SELECT replace(collate('r世eplace','UTF8_BINARY'),collate('pl','UTF8_BINARY'),collate('123','UTF8_BINARY'));
SELECT replace(collate('r世eplace','UTF8_BINARY'),'pl','123');
SELECT replace('r世eplace',collate('pl','UTF8_BINARY'),'123');
SELECT replace('r世eplace','pl',collate('123','UTF8_BINARY'));
SELECT replace(collate('repl世ace','UTF8_LCASE'),collate('PL','UTF8_LCASE'),collate('AB','UTF8_LCASE'));
SELECT replace(collate('repl世ace','UTF8_LCASE'),'PL','AB');
SELECT replace('repl世ace',collate('PL','UTF8_LCASE'),'AB');
SELECT replace('repl世ace','PL',collate('AB','UTF8_LCASE'));
SELECT replace(collate('abcdabcd','UNICODE'),collate('bc','UNICODE'),collate('','UNICODE'));
SELECT replace(collate('abcdabcd','UNICODE'),'bc','');
SELECT replace('abcdabcd',collate('bc','UNICODE'),'');
SELECT replace('abcdabcd','bc',collate('','UNICODE'));
SELECT replace(collate('aBc世abc','UNICODE_CI'),collate('b','UNICODE_CI'),collate('12','UNICODE_CI'));
SELECT replace(collate('aBc世abc','UNICODE_CI'),'b','12');
SELECT replace('aBc世abc',collate('b','UNICODE_CI'),'12');
SELECT replace('aBc世abc','b',collate('12','UNICODE_CI'));
SELECT replace(collate('abi̇o12i̇o','UNICODE_CI'),collate('İo','UNICODE_CI'),collate('yy','UNICODE_CI'));
SELECT replace(collate('abi̇o12i̇o','UNICODE_CI'),'İo','yy');
SELECT replace('abi̇o12i̇o',collate('İo','UNICODE_CI'),'yy');
SELECT replace('abi̇o12i̇o','İo',collate('yy','UNICODE_CI'));
SELECT replace(collate('abİo12i̇o','UNICODE_CI'),collate('i̇o','UNICODE_CI'),collate('xx','UNICODE_CI'));
SELECT replace(collate('abİo12i̇o','UNICODE_CI'),'i̇o','xx');
SELECT replace('abİo12i̇o',collate('i̇o','UNICODE_CI'),'xx');
SELECT replace('abİo12i̇o','i̇o',collate('xx','UNICODE_CI'));
SELECT startswith(collate('abcde', 'UTF8_LCASE'), collate('C', 'UNICODE_CI'));

-- EndsWith
SELECT endswith(collate('', 'UTF8_BINARY'), collate('', 'UTF8_BINARY'));
SELECT endswith(collate('', 'UTF8_BINARY'),'');
SELECT endswith('', collate('', 'UTF8_BINARY'));
SELECT endswith(collate('abcde', 'UNICODE'), collate('E', 'UNICODE'));
SELECT endswith(collate('abcde', 'UNICODE'),'E');
SELECT endswith('abcde', collate('E', 'UNICODE'));
SELECT endswith(collate('abcde', 'UTF8_LCASE'), collate('FGH', 'UTF8_LCASE'));
SELECT endswith(collate('abcde', 'UTF8_LCASE'),'FGH');
SELECT endswith('abcde', collate('FGH', 'UTF8_LCASE'));
SELECT endswith(collate('abcde', 'UNICODE_CI'), collate('CDE', 'UNICODE_CI'));
SELECT endswith(collate('abcde', 'UNICODE_CI'),'CDE');
SELECT endswith('abcde', collate('CDE', 'UNICODE_CI'));
SELECT endswith(collate('abcde', 'UTF8_LCASE'), collate('C', 'UNICODE_CI'));

-- StringRepeat
SELECT repeat(collate('', 'UTF8_BINARY'), 1);
SELECT repeat(collate('a', 'UNICODE'), 0);
SELECT repeat(collate('XY', 'UTF8_LCASE'), 3);
SELECT repeat(collate('123', 'UNICODE_CI'), 2);

-- Ascii & UnBase64 string expressions
select ascii('a' collate utf8_binary);
select ascii('B' collate utf8_lcase);
select ascii('#' collate unicode);
select ascii('!' collate unicode_ci);
select unbase64('QUJD' collate utf8_binary);
select unbase64('eHl6' collate utf8_lcase);
select unbase64('IyMj' collate utf8_binary);
select unbase64('IQ==' collate utf8_lcase);

-- Chr, Base64, Decode & FormatNumber
select chr(97);
select chr(66);
select base64('xyz');
select base64('!');
select decode(encode('$', 'utf-8'), 'utf-8');
select decode(encode('X', 'utf-8'), 'utf-8');
select format_number(123.123, '###.###');
select format_number(99.99, '##.##');

-- Encode, ToBinary & Sentences
select encode('a' collate utf8_binary, 'utf-8');
select encode('$' collate utf8_lcase, 'utf-8');
select to_binary('B' collate unicode, 'utf-8');
select to_binary('#' collate unicode_ci, 'utf-8');
select sentences('Hello, world! Nice day.' collate utf8_binary);
select sentences('Something else. Nothing here.' collate utf8_lcase);

-- Upper
SELECT upper(collate('aBc', 'UTF8_BINARY'));
SELECT upper(collate('aBc', 'UTF8_LCASE'));
SELECT upper(collate('aBc', 'UNICODE'));
SELECT upper(collate('aBc', 'UNICODE_CI'));

-- Lower
SELECT lower(collate('aBc', 'UTF8_BINARY'));
SELECT lower(collate('aBc', 'UTF8_LCASE'));
SELECT lower(collate('aBc', 'UNICODE'));
SELECT lower(collate('aBc', 'UNICODE_CI'));

-- InitCap
SELECT initcap(collate('aBc ABc', 'UTF8_BINARY'));
SELECT initcap(collate('aBc ABc', 'UTF8_LCASE'));
SELECT initcap(collate('aBc ABc', 'UNICODE'));
SELECT initcap(collate('aBc ABc', 'UNICODE_CI'));

-- Overlay
select overlay(collate('hello', 'UTF8_BINARY') placing collate(' world', 'UTF8_BINARY') from 6);
select overlay(collate('hello', 'UTF8_BINARY') placing ' world' from 6);
select overlay('hello' placing collate(' world', 'UTF8_BINARY') from 6);
select overlay(collate('hello', 'UTF8_BINARY') placing ' world' from collate('6', 'UTF8_BINARY'));
select overlay(collate('nice', 'UTF8_LCASE') placing collate(' day', 'UTF8_LCASE') from 5);
select overlay(collate('nice', 'UTF8_LCASE') placing ' day' from 5);
select overlay('nice' placing collate(' day', 'UTF8_LCASE') from 5);
select overlay(collate('nice', 'UTF8_LCASE') placing ' day' from collate('5', 'UTF8_LCASE'));
select overlay(collate('A', 'UNICODE') placing collate('B', 'UNICODE') from 1);
select overlay(collate('A', 'UNICODE') placing 'B' from 1);
select overlay('A' placing collate('B', 'UNICODE') from 1);
select overlay(collate('A', 'UNICODE') placing 'B' from collate('1', 'UNICODE'));
select overlay(collate('!', 'UNICODE_CI') placing collate('!!!', 'UNICODE_CI') from 1);
select overlay(collate('!', 'UNICODE_CI') placing '!!!' from 1);
select overlay('!' placing collate('!!!', 'UNICODE_CI') from 1);
select overlay(collate('!', 'UNICODE_CI') placing '!!!' from collate('1', 'UNICODE_CI'));
SELECT overlay('a' collate UNICODE PLACING 'b' collate UNICODE_CI FROM 1);

-- FormatString
select format_string(collate('%s%s', 'UTF8_BINARY'), 'a', 'b');
select format_string(collate('%d', 'UTF8_LCASE'), 123);
select format_string(collate('%s%d', 'UNICODE'), 'A', 0);
select format_string(collate('%s%s', 'UNICODE_CI'), 'Hello', '!!!');

-- SoundEx
select soundex('A' collate utf8_binary);
select soundex('!' collate utf8_lcase);
select soundex('$' collate unicode);
select soundex('X' collate unicode_ci);

-- Length, BitLength & OctetLength
select length('hello' collate utf8_binary);
select length('world' collate utf8_lcase);
select length('ﬀ' collate unicode);
select bit_length('hello' collate unicode_ci);
select bit_length('world' collate utf8_binary);
select bit_length('ﬀ' collate utf8_lcase);
select octet_length('hello' collate unicode);
select octet_length('world' collate unicode_ci);
select octet_length('ﬀ' collate utf8_binary);

-- Luhncheck
select luhn_check(123);
select luhn_check(000);
select luhn_check(111);
select luhn_check(222);

-- Levenshtein
select levenshtein('kitten' collate UTF8_BINARY, 'sitTing');
select levenshtein('kitten' collate UTF8_LCASE, 'sitTing');
select levenshtein('kitten' collate UNICODE, 'sitTing', 3);
select levenshtein('kitten' collate UNICODE_CI, 'sitTing', 3);

-- IsValidUTF8
SELECT is_valid_utf8(null collate UTF8_BINARY);
SELECT is_valid_utf8('' collate UTF8_LCASE);
SELECT is_valid_utf8('abc' collate UNICODE);
SELECT is_valid_utf8(x'FF' collate UNICODE_CI);

-- MakeValidUTF8
SELECT make_valid_utf8(null collate UTF8_BINARY);
SELECT make_valid_utf8('' collate UTF8_LCASE);
SELECT make_valid_utf8('abc' collate UNICODE);
SELECT make_valid_utf8(x'FF' collate UNICODE_CI);

-- ValidateUTF8
SELECT validate_utf8(null collate UTF8_BINARY);
SELECT validate_utf8('' collate UTF8_LCASE);
SELECT validate_utf8('abc' collate UNICODE);
SELECT validate_utf8(x'FF' collate UNICODE_CI);

-- TryValidateUTF8
SELECT try_validate_utf8(null collate UTF8_BINARY);
SELECT try_validate_utf8('' collate UTF8_LCASE);
SELECT try_validate_utf8('abc' collate UNICODE);
SELECT try_validate_utf8(x'FF' collate UNICODE_CI);

-- Left/Right/Substr
SELECT substr(collate('example', 'utf8_lcase'), 1, '100');
SELECT substr(collate('example', 'utf8_binary'), 2, '2');
SELECT right(collate('', 'utf8_lcase'), 1);
SELECT substr(collate('example', 'unicode'), 0, '0');
SELECT substr(collate('example', 'unicode_ci'), -3, '2');
SELECT substr(collate(' a世a ', 'utf8_lcase'), 2, '3');
SELECT left(collate(' a世a ', 'utf8_binary'), 3);
SELECT right(collate(' a世a ', 'unicode'), 3);
SELECT left(collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ', 'unicode_ci'), 3);
SELECT right(collate('ÀÃÂĀĂȦÄäâãȻȻȻȻȻǢǼÆ', 'utf8_lcase'), 3);
SELECT substr(collate('', 'utf8_lcase'), 1, '1');
SELECT substr(collate('', 'unicode'), 1, '1');
SELECT left(collate('', 'utf8_binary'), 1);
SELECT left(collate(null, 'utf8_lcase'), 1);
SELECT right(collate(null, 'unicode'), 1);
SELECT substr(collate(null, 'utf8_binary'), 1);
SELECT substr(collate(null, 'unicode_ci'), 1, '1');
SELECT left(collate(null, 'utf8_lcase'), null);
SELECT right(collate(null, 'unicode'), null);
SELECT substr(collate(null, 'utf8_binary'), null, 'null');
SELECT substr(collate(null, 'unicode_ci'), null);
SELECT left(collate('ÀÃÂȦÄäåäáâãȻȻȻǢǼÆ', 'utf8_lcase'), null);
SELECT right(collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ', 'unicode'), null);
SELECT substr(collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ', 'utf8_binary'), null);
SELECT substr(collate('', 'unicode_ci'), null);

-- StringRPad
SELECT rpad(collate('', 'UTF8_BINARY'), 5, collate(' ', 'UTF8_BINARY'));
SELECT rpad(collate('', 'UTF8_BINARY'), 5, ' ');
SELECT rpad('', 5, collate(' ', 'UTF8_BINARY'));
SELECT rpad(collate('abc', 'UNICODE'), 5, collate(' ', 'UNICODE'));
SELECT rpad(collate('abc', 'UNICODE'), 5, ' ');
SELECT rpad('abc', 5, collate(' ', 'UNICODE'));
SELECT rpad(collate('Hello', 'UTF8_LCASE'), 7, collate('Wörld', 'UTF8_LCASE'));
SELECT rpad(collate('Hello', 'UTF8_LCASE'), 7, 'Wörld');
SELECT rpad('Hello', 7, collate('Wörld', 'UTF8_LCASE'));
SELECT rpad(collate('1234567890', 'UNICODE_CI'), 5, collate('aaaAAa', 'UNICODE_CI'));
SELECT rpad(collate('1234567890', 'UNICODE_CI'), 5, 'aaaAAa');
SELECT rpad('1234567890', 5, collate('aaaAAa', 'UNICODE_CI'));
SELECT rpad(collate('aaAA', 'UTF8_BINARY'), 2, collate(' ', 'UTF8_BINARY'));
SELECT rpad(collate('aaAA', 'UTF8_BINARY'), 2, ' ');
SELECT rpad('aaAA', 2, collate(' ', 'UTF8_BINARY'));
SELECT rpad(collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃', 'UTF8_LCASE'), 2, collate('1', 'UTF8_LCASE'));
SELECT rpad(collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃', 'UTF8_LCASE'), 2, '1');
SELECT rpad('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃', 2, collate('1', 'UTF8_LCASE'));
SELECT rpad(collate('ĂȦÄäåäá', 'UNICODE'), 20, collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ', 'UNICODE'));
SELECT rpad(collate('ĂȦÄäåäá', 'UNICODE'), 20, 'ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ');
SELECT rpad('ĂȦÄäåäá', 20, collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ', 'UNICODE'));
SELECT rpad(collate('aȦÄä', 'UNICODE_CI'), 8, collate('a1', 'UNICODE_CI'));
SELECT rpad(collate('aȦÄä', 'UNICODE_CI'), 8, 'a1');
SELECT rpad('aȦÄä', 8, collate('a1', 'UNICODE_CI'));
SELECT rpad(collate('abcde', 'UNICODE_CI'), 1, collate('C', 'UTF8_LCASE'));

-- StringLPad
SELECT lpad(collate('', 'UTF8_BINARY'), 5, collate(' ', 'UTF8_BINARY'));
SELECT lpad(collate('', 'UTF8_BINARY'), 5, ' ');
SELECT lpad('', 5, collate(' ', 'UTF8_BINARY'));
SELECT lpad(collate('abc', 'UNICODE'), 5, collate(' ', 'UNICODE'));
SELECT lpad(collate('abc', 'UNICODE'), 5, ' ');
SELECT lpad('abc', 5, collate(' ', 'UNICODE'));
SELECT lpad(collate('Hello', 'UTF8_LCASE'), 7, collate('Wörld', 'UTF8_LCASE'));
SELECT lpad(collate('Hello', 'UTF8_LCASE'), 7, 'Wörld');
SELECT lpad('Hello', 7, collate('Wörld', 'UTF8_LCASE'));
SELECT lpad(collate('1234567890', 'UNICODE_CI'), 5, collate('aaaAAa', 'UNICODE_CI'));
SELECT lpad(collate('1234567890', 'UNICODE_CI'), 5, 'aaaAAa');
SELECT lpad('1234567890', 5, collate('aaaAAa', 'UNICODE_CI'));
SELECT lpad(collate('aaAA', 'UTF8_BINARY'), 2, collate(' ', 'UTF8_BINARY'));
SELECT lpad(collate('aaAA', 'UTF8_BINARY'), 2, ' ');
SELECT lpad('aaAA', 2, collate(' ', 'UTF8_BINARY'));
SELECT lpad(collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃', 'UTF8_LCASE'), 2, collate('1', 'UTF8_LCASE'));
SELECT lpad(collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃', 'UTF8_LCASE'), 2, '1');
SELECT lpad('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ℀℃', 2, collate('1', 'UTF8_LCASE'));
SELECT lpad(collate('ĂȦÄäåäá', 'UNICODE'), 20, collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ', 'UNICODE'));
SELECT lpad(collate('ĂȦÄäåäá', 'UNICODE'), 20, 'ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ');
SELECT lpad('ĂȦÄäåäá', 20, collate('ÀÃÂĀĂȦÄäåäáâãȻȻȻȻȻǢǼÆ', 'UNICODE'));
SELECT lpad(collate('aȦÄä', 'UNICODE_CI'), 8, collate('a1', 'UNICODE_CI'));
SELECT lpad(collate('aȦÄä', 'UNICODE_CI'), 8, 'a1');
SELECT lpad('aȦÄä', 8, collate('a1', 'UNICODE_CI'));
SELECT lpad(collate('abcde', 'UNICODE_CI'), 1, collate('C', 'UTF8_LCASE'));
SELECT lpad('abc', collate('5', 'unicode_ci'), ' ');

-- Locate
SELECT locate(collate('aa','UTF8_BINARY'),collate('aaads','UTF8_BINARY'),0);
SELECT locate(collate('aa','UTF8_BINARY'),'aaads',0);
SELECT locate('aa',collate('aaads','UTF8_BINARY'),0);
SELECT locate(collate('aa','UTF8_LCASE'),collate('Aaads','UTF8_LCASE'),0);
SELECT locate(collate('aa','UTF8_LCASE'),'Aaads',0);
SELECT locate('aa',collate('Aaads','UTF8_LCASE'),0);
SELECT locate(collate('界x','UTF8_LCASE'),collate('test大千世界X大千世界','UTF8_LCASE'),1);
SELECT locate(collate('界x','UTF8_LCASE'),'test大千世界X大千世界',1);
SELECT locate('界x',collate('test大千世界X大千世界','UTF8_LCASE'),1);
SELECT locate(collate('aBc','UTF8_LCASE'),collate('abcabc','UTF8_LCASE'),4);
SELECT locate(collate('aBc','UTF8_LCASE'),'abcabc',4);
SELECT locate('aBc',collate('abcabc','UTF8_LCASE'),4);
SELECT locate(collate('aa','UNICODE'),collate('Aaads','UNICODE'),0);
SELECT locate(collate('aa','UNICODE'),'Aaads',0);
SELECT locate('aa',collate('Aaads','UNICODE'),0);
SELECT locate(collate('abC','UNICODE'),collate('abCabC','UNICODE'),2);
SELECT locate(collate('abC','UNICODE'),'abCabC',2);
SELECT locate('abC',collate('abCabC','UNICODE'),2);
SELECT locate(collate('aa','UNICODE_CI'),collate('Aaads','UNICODE_CI'),0);
SELECT locate(collate('aa','UNICODE_CI'),'Aaads',0);
SELECT locate('aa',collate('Aaads','UNICODE_CI'),0);
SELECT locate(collate('界x','UNICODE_CI'),collate('test大千世界X大千世界','UNICODE_CI'),1);
SELECT locate(collate('界x','UNICODE_CI'),'test大千世界X大千世界',1);
SELECT locate('界x',collate('test大千世界X大千世界','UNICODE_CI'),1);
SELECT locate(collate('aBc', 'UTF8_BINARY'), collate('abcabc', 'UTF8_LCASE'), 4);

-- StringTrim*
SELECT TRIM(COLLATE('  asd  ', 'UTF8_BINARY'));
SELECT BTRIM(COLLATE('  asd  ', 'UTF8_BINARY'), null);
SELECT LTRIM('x', COLLATE('xxasdxx', 'UTF8_BINARY'));
SELECT RTRIM('x', COLLATE('xxasdxx', 'UTF8_BINARY'));
SELECT TRIM(null, COLLATE('  asd  ', 'UTF8_LCASE'));
SELECT BTRIM(COLLATE('xxasdxx', 'UTF8_LCASE'), 'x');
SELECT LTRIM('x', COLLATE('xxasdxx', 'UTF8_LCASE'));
SELECT RTRIM(COLLATE('  asd  ', 'UTF8_LCASE'));
SELECT TRIM('x', COLLATE('xxasdxx', 'UTF8_BINARY'));
SELECT BTRIM(COLLATE('xxasdxx', 'UTF8_BINARY'), 'x');
SELECT LTRIM(COLLATE('  asd  ', 'UTF8_BINARY'));
SELECT RTRIM(null, COLLATE('  asd  ', 'UTF8_BINARY'));

-- StringTrim* - implicit collations
SELECT TRIM(COLLATE('x', 'UTF8_BINARY'), COLLATE('xax', 'UTF8_BINARY'));
SELECT BTRIM(COLLATE('xax', 'UTF8_LCASE'), COLLATE('x', 'UTF8_LCASE'));
SELECT LTRIM(COLLATE('x', 'UTF8_BINARY'), COLLATE('xax', 'UTF8_BINARY'));
SELECT RTRIM('x', COLLATE('xax', 'UTF8_BINARY'));
SELECT TRIM('x', COLLATE('xax', 'UTF8_LCASE'));
SELECT BTRIM('xax', COLLATE('x', 'UTF8_BINARY'));
SELECT LTRIM(COLLATE('x', 'UTF8_BINARY'), 'xax');
SELECT RTRIM(COLLATE('x', 'UTF8_LCASE'), 'xax');
SELECT TRIM(COLLATE('x', 'UTF8_BINARY'), 'xax');

-- StringTrim* - collation type mismatch
SELECT TRIM(COLLATE('x', 'UTF8_LCASE'), COLLATE('xxaaaxx', 'UTF8_BINARY'));
SELECT LTRIM(COLLATE('x', 'UTF8_LCASE'), COLLATE('xxaaaxx', 'UTF8_BINARY'));
SELECT RTRIM(COLLATE('x', 'UTF8_LCASE'), COLLATE('xxaaaxx', 'UTF8_BINARY'));
SELECT BTRIM(COLLATE('xxaaaxx', 'UTF8_BINARY'), COLLATE('x', 'UTF8_LCASE'));