-- test cases for padding and trimming with collations

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

-- StringRPad
select rpad(utf8_binary, 8, utf8_lcase) from t1;
select rpad(s, 8, utf8_binary) from t1;
select rpad(utf8_binary collate utf8_binary, 8, s collate utf8_lcase) from t1;
select rpad(utf8_binary, 8, utf8_lcase collate utf8_binary) from t1;
select rpad(utf8_binary collate utf8_lcase, 8, utf8_lcase collate utf8_lcase) from t1;
select lpad(utf8_binary collate utf8_binary_rtrim, 8, utf8_lcase collate utf8_binary_rtrim) from t1;
select rpad(utf8_binary, 8, 'a'), rpad(utf8_lcase, 8, 'a') from t1;
select rpad(utf8_binary, 8, 'AaAA' collate utf8_lcase), rpad(utf8_lcase, 8, 'AAa' collate utf8_binary) from t1;

-- StringLPad
select lpad(utf8_binary, 8, utf8_lcase) from t1;
select lpad(s, 8, utf8_binary) from t1;
select lpad(utf8_binary collate utf8_binary, 8, s collate utf8_lcase) from t1;
select lpad(utf8_binary, 8, utf8_lcase collate utf8_binary) from t1;
select lpad(utf8_binary collate utf8_lcase, 8, utf8_lcase collate utf8_lcase) from t1;
select lpad(utf8_binary collate utf8_binary_rtrim, 8, utf8_lcase collate utf8_binary_rtrim) from t1;
select lpad(utf8_binary, 8, 'a'), lpad(utf8_lcase, 8, 'a') from t1;
select lpad(utf8_binary, 8, 'AaAA' collate utf8_lcase), lpad(utf8_lcase, 8, 'AAa' collate utf8_binary) from t1;

-- StringTrim
select TRIM(utf8_binary, utf8_lcase) from t1;
select TRIM(s, utf8_binary) from t1;
select TRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select TRIM(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select TRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select TRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select TRIM(utf8_binary collate utf8_binary_rtrim, utf8_lcase collate utf8_binary_rtrim) from t1;
select TRIM('ABc', utf8_binary), TRIM('ABc', utf8_lcase) from t1;
select TRIM('ABc' collate utf8_lcase, utf8_binary), TRIM('AAa' collate utf8_binary, utf8_lcase) from t1;
-- StringTrimBoth
select BTRIM(utf8_binary, utf8_lcase) from t1;
select BTRIM(s, utf8_binary) from t1;
select BTRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select BTRIM(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select BTRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select BTRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select BTRIM(utf8_binary collate utf8_binary_rtrim, utf8_lcase collate utf8_binary_rtrim) from t1;
select BTRIM('ABc', utf8_binary), BTRIM('ABc', utf8_lcase) from t1;
select BTRIM('ABc' collate utf8_lcase, utf8_binary), BTRIM('AAa' collate utf8_binary, utf8_lcase) from t1;
-- StringTrimLeft
select LTRIM(utf8_binary, utf8_lcase) from t1;
select LTRIM(s, utf8_binary) from t1;
select LTRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select LTRIM(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select LTRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select LTRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select LTRIM(utf8_binary collate utf8_binary_rtrim, utf8_lcase collate utf8_binary_rtrim) from t1;
select LTRIM('ABc', utf8_binary), LTRIM('ABc', utf8_lcase) from t1;
select LTRIM('ABc' collate utf8_lcase, utf8_binary), LTRIM('AAa' collate utf8_binary, utf8_lcase) from t1;
-- StringTrimRight
select RTRIM(utf8_binary, utf8_lcase) from t1;
select RTRIM(s, utf8_binary) from t1;
select RTRIM(utf8_binary collate utf8_binary, s collate utf8_lcase) from t1;
select RTRIM(utf8_binary, utf8_lcase collate utf8_binary) from t1;
select RTRIM(utf8_binary collate utf8_lcase, utf8_lcase collate utf8_lcase) from t1;
select RTRIM(utf8_binary collate unicode_ai, utf8_lcase collate unicode_ai) from t1;
select RTRIM(utf8_binary collate utf8_binary_rtrim, utf8_lcase collate utf8_binary_rtrim) from t1;
select RTRIM('ABc', utf8_binary), RTRIM('ABc', utf8_lcase) from t1;
select RTRIM('ABc' collate utf8_lcase, utf8_binary), RTRIM('AAa' collate utf8_binary, utf8_lcase) from t1;

drop table t1;
