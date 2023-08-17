--- TESTS FOR DATETIME FORMATTING FUNCTIONS ---

create temporary view v as select col from values
 (timestamp '1582-06-01 11:33:33.123UTC+080000'),
 (timestamp '1970-01-01 00:00:00.000Europe/Paris'),
 (timestamp '1970-12-31 23:59:59.999Asia/Srednekolymsk'),
 (timestamp '1996-04-01 00:33:33.123Australia/Darwin'),
 (timestamp '2018-11-17 13:33:33.123Z'),
 (timestamp '2020-01-01 01:33:33.123Asia/Shanghai'),
 (timestamp '2100-01-01 01:33:33.123America/Los_Angeles') t(col);

select col, date_format(col, 'G GG GGG GGGG'), to_char(col, 'G GG GGG GGGG') from v;

select col, date_format(col, 'y yy yyy yyyy yyyyy yyyyyy'), to_char(col, 'y yy yyy yyyy yyyyy yyyyyy') from v;

select col, date_format(col, 'q qq'), to_char(col, 'q qq') from v;

select col, date_format(col, 'Q QQ QQQ QQQQ'), to_char(col, 'Q QQ QQQ QQQQ') from v;

select col, date_format(col, 'M MM MMM MMMM'), to_char(col, 'M MM MMM MMMM') from v;

select col, date_format(col, 'L LL'), to_char(col, 'L LL') from v;

select col, date_format(col, 'E EE EEE EEEE'), to_char(col, 'E EE EEE EEEE') from v;

select col, date_format(col, 'F'), to_char(col, 'F') from v;

select col, date_format(col, 'd dd'), to_char(col, 'd dd') from v;

select col, date_format(col, 'DD'), to_char(col, 'DD') from v where col = timestamp '2100-01-01 01:33:33.123America/Los_Angeles';
select col, date_format(col, 'D DDD'), to_char(col, 'D DDD') from v;

select col, date_format(col, 'H HH'), to_char(col, 'H HH') from v;

select col, date_format(col, 'h hh'), to_char(col, 'h hh') from v;

select col, date_format(col, 'k kk'), to_char(col, 'k kk') from v;

select col, date_format(col, 'K KK'), to_char(col, 'K KK') from v;

select col, date_format(col, 'm mm'), to_char(col, 'm mm') from v;

select col, date_format(col, 's ss'), to_char(col, 's ss') from v;

select col, date_format(col, 'S SS SSS SSSS SSSSS SSSSSS SSSSSSS SSSSSSSS SSSSSSSSS'), to_char(col, 'S SS SSS SSSS SSSSS SSSSSS SSSSSSS SSSSSSSS SSSSSSSSS') from v;

select col, date_format(col, 'a'), to_char(col, 'a') from v;

select col, date_format(col, 'VV'), to_char(col, 'VV') from v;

select col, date_format(col, 'z zz zzz zzzz'), to_char(col, 'z zz zzz zzzz') from v;

select col, date_format(col, 'X XX XXX'), to_char(col, 'X XX XXX') from v;
select col, date_format(col, 'XXXX XXXXX'), to_char(col, 'XXXX XXXXX') from v;

select col, date_format(col, 'Z ZZ ZZZ ZZZZ ZZZZZ'), to_char(col, 'Z ZZ ZZZ ZZZZ ZZZZZ') from v;

select col, date_format(col, 'O OOOO'), to_char(col, 'O OOOO') from v;

select col, date_format(col, 'x xx xxx xxxx xxxx xxxxx'), to_char(col, 'x xx xxx xxxx xxxx xxxxx') from v;

-- optional pattern, but the results won't be optional for formatting
select col, date_format(col, '[yyyy-MM-dd HH:mm:ss]'), to_char(col, '[yyyy-MM-dd HH:mm:ss]') from v;

-- literals
select col, date_format(col, "姚123'GyYqQMLwWuEFDdhHmsSaVzZxXOV'"), to_char(col, "姚123'GyYqQMLwWuEFDdhHmsSaVzZxXOV'") from v;
select col, date_format(col, "''"), to_char(col, "''") from v;
select col, date_format(col, ''), to_char(col, '') from v;
