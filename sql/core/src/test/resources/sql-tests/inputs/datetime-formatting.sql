--- TESTS FOR DATETIME FORMATTING FUNCTIONS ---

-- valid formatter pattern check
create temporary view v as select t from values
 (timestamp '1582-06-01 11:33:33.123UTC+080000'),
 (timestamp '1970-01-01 00:00:00.000Europe/Paris'),
 (timestamp '1970-12-31 23:59:59.999Asia/Srednekolymsk'),
 (timestamp '1996-04-01 00:33:33.123Australia/Darwin'),
 (timestamp '2018-11-17 13:33:33.123Z'),
 (timestamp '2020-01-01 01:33:33.123Asia/Shanghai'),
 (timestamp '2100-01-01 01:33:33.123America/Los_Angeles') t(col);

select col, date_format(col, 'Y-w-u YYYY-ww-uu YYY-W-uuu YY YYYYY uuuu E EE EEE EEEE') from v;
select col, date_format(col, 'q qq Q QQ QQQ QQQQ') from v;
select col, date_format(col, 'y-M-d H:m:s yyyy-MM-dd HH:mm:ss.SSS yy yyy yyyyy MMM MMMM L LL F h hh k kk K KK a') from v;
select col, date_format(col, 'z zz zzz zzzz X XX XXX  Z ZZ ZZZ ZZZZ ZZZZZ') from v;
-- These patterns for time zone is unsupported by the legacy formatter
select col, date_format(col, 'VV O OOOO XXXX XXXXX x xx xxx xxxx xxxx xxxxx') from v;
select date_format(date '1970-01-01', 'D DD DDD');
