--- TESTS FOR DATETIME FORMATTING FUNCTIONS ---

create temporary view v as select col from values
 (timestamp '1582-06-01 11:33:33.123UTC+080000'),
 (timestamp '1970-01-01 00:00:00.000Europe/Paris'),
 (timestamp '1970-12-31 23:59:59.999Asia/Srednekolymsk'),
 (timestamp '1996-04-01 00:33:33.123Australia/Darwin'),
 (timestamp '2018-11-17 13:33:33.123Z'),
 (timestamp '2020-01-01 01:33:33.123Asia/Shanghai'),
 (timestamp '2100-01-01 01:33:33.123America/Los_Angeles') t(col);

select col, date_format(col, 'G GG GGG GGGG') from v;

select col, date_format(col, 'y yy yyy yyyy yyyyy yyyyyy') from v;

select col, date_format(col, 'q qq') from v;

select col, date_format(col, 'Q QQ QQQ QQQQ') from v;

select col, date_format(col, 'M MM MMM MMMM') from v;

select col, date_format(col, 'L LL') from v;

select col, date_format(col, 'E EE EEE EEEE') from v;

select col, date_format(col, 'F') from v;

select col, date_format(col, 'd dd') from v;

select col, date_format(col, 'DD') from v where col = timestamp '2100-01-01 01:33:33.123America/Los_Angeles';
select col, date_format(col, 'D DDD') from v;

select col, date_format(col, 'H HH') from v;

select col, date_format(col, 'h hh') from v;

select col, date_format(col, 'k kk') from v;

select col, date_format(col, 'K KK') from v;

select col, date_format(col, 'm mm') from v;

select col, date_format(col, 's ss') from v;

select col, date_format(col, 'S SS SSS SSSS SSSSS SSSSSS SSSSSSS SSSSSSSS SSSSSSSSS') from v;

select col, date_format(col, 'a') from v;

select col, date_format(col, 'VV') from v;

select col, date_format(col, 'z zz zzz zzzz') from v;

select col, date_format(col, 'X XX XXX') from v;
select col, date_format(col, 'XXXX XXXXX') from v;

select col, date_format(col, 'Z ZZ ZZZ ZZZZ ZZZZZ') from v;

select col, date_format(col, 'O OOOO') from v;

select col, date_format(col, 'x xx xxx xxxx xxxx xxxxx') from v;

-- optional pattern, but the results won't be optional for formatting
select col, date_format(col, '[yyyy-MM-dd HH:mm:ss]') from v;

-- literals
select col, date_format(col, "姚123'GyYqQMLwWuEFDdhHmsSaVzZxXOV'") from v;
select col, date_format(col, "''") from v;
select col, date_format(col, '') from v;
