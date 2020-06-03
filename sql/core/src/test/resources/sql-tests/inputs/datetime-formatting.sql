--- TESTS FOR DATETIME FORMATTING FUNCTIONS ---

create temporary view v as select col from values
 (timestamp '1582-06-01 11:33:33.123UTC+080000'),
 (timestamp '1970-01-01 00:00:00.000Europe/Paris'),
 (timestamp '1970-12-31 23:59:59.999Asia/Srednekolymsk'),
 (timestamp '1996-04-01 00:33:33.123Australia/Darwin'),
 (timestamp '2018-11-17 13:33:33.123Z'),
 (timestamp '2020-01-01 01:33:33.123Asia/Shanghai'),
 (timestamp '2100-01-01 01:33:33.123America/Los_Angeles') t(col);

---------------------------  '1 12 123 1234 12345 123456 1234567 12345678 123456789 1234567890'
select col, date_format(col, 'G GG GGG GGGG') from v;
select col, date_format(col, 'GGGGG') from v;

select col, date_format(col, 'y yy yyy yyyy yyyyy yyyyyy yyyyyyy yyyyyyyy yyyyyyyyy yyyyyyyyyy') from v;
select col, date_format(col, 'yyyyyyyyyyy') from v;

select col, date_format(col, 'Y YY YYY YYYY YYYYY YYYYYY YYYYYYY YYYYYYYY YYYYYYYYY YYYYYYYYYY') from v;
select col, date_format(col, 'YYYYYYYYYYY') from v;

select col, date_format(col, 'q qq') from v;
select col, date_format(col, 'qqq') from v;

select col, date_format(col, 'Q QQ QQQ QQQQ') from v;
select col, date_format(col, 'QQQQQ') from v;

select col, date_format(col, 'M MM MMM MMMM') from v;
select col, date_format(col, 'MMMMM') from v;

select col, date_format(col, 'L LL') from v;
select col, date_format(col, 'LLL') from v;

select col, date_format(col, 'w ww') from v;
select col, date_format(col, 'www') from v;

select col, date_format(col, 'W') from v;
select col, date_format(col, 'WW') from v;

select col, date_format(col, 'u uu uuu uuuu') from v;
select col, date_format(col, 'uuuuu') from v;

select col, date_format(col, 'E EE EEE EEEE') from v;
select col, date_format(col, 'EEEEE') from v;

select col, date_format(col, 'F') from v;
select col, date_format(col, 'FF') from v;

select col, date_format(col, 'd dd') from v;
select col, date_format(col, 'ddd') from v;

select col, date_format(col, 'DD') from v where col = timestamp '2100-01-01 01:33:33.123America/Los_Angeles';
select col, date_format(col, 'DD') from v;
select col, date_format(col, 'D DDD') from v;
select col, date_format(col, 'DDDD') from v;

select col, date_format(col, 'H HH') from v;
select col, date_format(col, 'HHH') from v;

select col, date_format(col, 'h hh') from v;
select col, date_format(col, 'hhh') from v;

select col, date_format(col, 'k kk') from v;
select col, date_format(col, 'kkk') from v;

select col, date_format(col, 'K KK') from v;
select col, date_format(col, 'KKK') from v;

select col, date_format(col, 'm mm') from v;
select col, date_format(col, 'mmm') from v;

select col, date_format(col, 's ss') from v;
select col, date_format(col, 'sss') from v;

select col, date_format(col, 'S SS SSS SSSS SSSSS SSSSSS SSSSSSS SSSSSSSS SSSSSSSSS') from v;
select col, date_format(col, 'SSSSSSSSSS') from v;

select col, date_format(col, 'a') from v;
select col, date_format(col, 'aa') from v;

select col, date_format(col, 'VV') from v;
select col, date_format(col, 'V') from v;

select col, date_format(col, 'z zz zzz zzzz') from v;
select col, date_format(col, 'zzzzz') from v;
select col, date_format(col, 'X XX XXX') from v;
select col, date_format(col, 'XXXX XXXXX') from v;
select col, date_format(col, 'XXXXXX') from v;
select col, date_format(col, 'Z ZZ ZZZ ZZZZ ZZZZZ') from v;
select col, date_format(col, 'ZZZZZZ') from v;
select col, date_format(col, 'O OOOO') from v;
select col, date_format(col, 'OO') from v;
select col, date_format(col, 'x xx xxx xxxx xxxx xxxxx') from v;
select col, date_format(col, 'xxxxxx') from v;
