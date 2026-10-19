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

-- Unary trim uses collation-aware space matching (SPARK-59633).
-- Under UNICODE_CI, NBSP (chr(160)) compares as equal to ASCII space, so unary trim removes it
-- and agrees with the explicit two-argument form. Under UNICODE (case-sensitive) it is preserved.
-- The trim string is the first argument: trim(trimStr, srcStr) removes trimStr from srcStr,
-- so the unary form below is equivalent to trim(' ', value). length/octet_length of the trimmed
-- result disambiguate NBSP (2 UTF-8 bytes) from ASCII space, which look identical in the output.
select trim(s), length(trim(s)), octet_length(trim(s))
  from (select concat(chr(160), 'abc', chr(160)) collate unicode_ci as s);
select trim(' ' collate unicode_ci, s), length(trim(' ' collate unicode_ci, s)),
       octet_length(trim(' ' collate unicode_ci, s))
  from (select concat(chr(160), 'abc', chr(160)) collate unicode_ci as s);
select ltrim(s), length(ltrim(s)), octet_length(ltrim(s))
  from (select concat(chr(160), 'abc', chr(160)) collate unicode_ci as s);
select rtrim(s), length(rtrim(s)), octet_length(rtrim(s))
  from (select concat(chr(160), 'abc', chr(160)) collate unicode_ci as s);
select trim(s), length(trim(s)), octet_length(trim(s))
  from (select concat(chr(160), 'abc', chr(160)) collate unicode as s);
select ltrim(s), length(ltrim(s)), octet_length(ltrim(s))
  from (select concat(chr(160), 'abc', chr(160)) collate unicode as s);
select rtrim(s), length(rtrim(s)), octet_length(rtrim(s))
  from (select concat(chr(160), 'abc', chr(160)) collate unicode as s);

drop table t1;
