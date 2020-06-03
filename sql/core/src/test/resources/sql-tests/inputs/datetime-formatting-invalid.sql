--- TESTS FOR DATETIME FORMATTING FUNCTIONS WITH INVALID PATTERNS ---

-- separating this from datetime-formatting.sql ,because the text form
-- for patterns with 5 letters in SimpleDateFormat varies from different JDKs
create temporary view v as select col from values
 (timestamp '1582-06-01 11:33:33.123UTC+080000'),
 (timestamp '1970-01-01 00:00:00.000Europe/Paris'),
 (timestamp '1970-12-31 23:59:59.999Asia/Srednekolymsk'),
 (timestamp '1996-04-01 00:33:33.123Australia/Darwin'),
 (timestamp '2018-11-17 13:33:33.123Z'),
 (timestamp '2020-01-01 01:33:33.123Asia/Shanghai'),
 (timestamp '2100-01-01 01:33:33.123America/Los_Angeles') t(col);

select col, date_format(col, 'GGGGG') from v;
select col, date_format(col, 'yyyyyyyyyyy') from v; -- pattern letter count can not be greater than 10
select col, date_format(col, 'YYYYYYYYYYY') from v;
-- q/L in JDK 8 will fail when the count is more than 2
select col, date_format(col, 'qqqqq') from v;
select col, date_format(col, 'QQQQQ') from v;
select col, date_format(col, 'MMMMM') from v;
select col, date_format(col, 'LLLLL') from v;
select col, date_format(col, 'www') from v;
select col, date_format(col, 'WW') from v;
select col, date_format(col, 'uuuuu') from v;
select col, date_format(col, 'EEEEE') from v;
select col, date_format(col, 'FF') from v;
select col, date_format(col, 'ddd') from v;
select col, date_format(col, 'DD') from v;
select col, date_format(col, 'DDDD') from v;
select col, date_format(col, 'HHH') from v;
select col, date_format(col, 'hhh') from v;
select col, date_format(col, 'kkk') from v;
select col, date_format(col, 'KKK') from v;
select col, date_format(col, 'mmm') from v;
select col, date_format(col, 'sss') from v;
select col, date_format(col, 'SSSSSSSSSS') from v;
select col, date_format(col, 'aa') from v;
select col, date_format(col, 'V') from v;
select col, date_format(col, 'zzzzz') from v;
select col, date_format(col, 'XXXXXX') from v;
select col, date_format(col, 'ZZZZZZ') from v;
select col, date_format(col, 'OO') from v;
select col, date_format(col, 'xxxxxx') from v;
