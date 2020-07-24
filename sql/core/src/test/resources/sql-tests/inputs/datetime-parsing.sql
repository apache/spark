--- TESTS FOR DATETIME PARSING FUNCTIONS ---

-- parsing with pattern 'y'
select to_timestamp('1', 'y');
select to_timestamp('123456', 'y');

select to_timestamp('12', 'yy');

select to_timestamp('123', 'yyy');
select to_timestamp('123456', 'yyy');

select to_timestamp('1234', 'yyyy');
select to_timestamp('12345', 'yyyyy');
select to_timestamp('123456', 'yyyyyy');

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
