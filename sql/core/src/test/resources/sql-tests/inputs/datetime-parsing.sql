--- TESTS FOR DATETIME PARSING FUNCTIONS ---

-- parsing with pattern 'y'
select to_timestamp('1', 'y');
select to_timestamp('12', 'y');
select to_timestamp('123', 'y');
select to_timestamp('1234', 'y');
select to_timestamp('12345', 'y');
select to_timestamp('123456', 'y');

select to_timestamp('12', 'yy');

select to_timestamp('123', 'yyy');
select to_timestamp('1234', 'yyy');
select to_timestamp('12345', 'yyy');
select to_timestamp('123456', 'yyy');

select to_timestamp('1', 'yyyy');
select to_timestamp('12', 'yyyy');
select to_timestamp('123', 'yyyy');
select to_timestamp('1234', 'yyyy');
select to_timestamp('12345', 'yyyy');
select to_timestamp('123456', 'yyyy');

select to_timestamp('1', 'yyyyy');
select to_timestamp('12', 'yyyyy');
select to_timestamp('123', 'yyyyy');
select to_timestamp('1234', 'yyyyy');
select to_timestamp('12345', 'yyyyy');
select to_timestamp('123456', 'yyyyy');

select to_timestamp('1', 'yyyyyy');
select to_timestamp('12', 'yyyyyy');
select to_timestamp('123', 'yyyyyy');
select to_timestamp('1234', 'yyyyyy');
select to_timestamp('12345', 'yyyyyy');
select to_timestamp('123456', 'yyyyyy');

select to_timestamp('1', 'yyyyyyy');
select to_timestamp('12', 'yyyyyyy');
select to_timestamp('123', 'yyyyyyy');
select to_timestamp('1234', 'yyyyyyy');
select to_timestamp('12345', 'yyyyyyy');
select to_timestamp('123456', 'yyyyyyy');

select to_timestamp('1', 'yyyyyyyy');
select to_timestamp('12', 'yyyyyyyy');
select to_timestamp('123', 'yyyyyyyy');
select to_timestamp('1234', 'yyyyyyyy');
select to_timestamp('12345', 'yyyyyyyy');
select to_timestamp('123456', 'yyyyyyyy');

select to_timestamp('1', 'yyyyyyyyyy');
select to_timestamp('12', 'yyyyyyyyyy');
select to_timestamp('123', 'yyyyyyyyyy');
select to_timestamp('1234', 'yyyyyyyyyy');
select to_timestamp('123456', 'yyyyyyyyyy');

select to_timestamp('1', 'yyyyyyyyyyy');
select to_timestamp('12', 'yyyyyyyyyyy');
select to_timestamp('123', 'yyyyyyyyyyy');
select to_timestamp('1234', 'yyyyyyyyyyy');
select to_timestamp('12345', 'yyyyyyyyyyy');
select to_timestamp('123456', 'yyyyyyyyyyy');

-- parsing with pattern 'D'
select to_timestamp('9', 'D');
select to_timestamp('300', 'D');
select to_timestamp('09', 'DD');
select to_timestamp('99', 'DD');
select to_timestamp('009', 'DDD');
select to_timestamp('365', 'DDD');
select to_timestamp('31-365', 'dd-DDD');
select to_timestamp('12-365', 'MM-DDD');
select to_timestamp('2020-365', 'yyyy-DDD');
select to_timestamp('12-31-365', 'MM-dd-DDD');
select to_timestamp('2020-30-365', 'yyyy-dd-DDD');
select to_timestamp('2020-12-350', 'yyyy-MM-DDD');
select to_timestamp('2020-12-31-366', 'yyyy-MM-dd-DDD');
