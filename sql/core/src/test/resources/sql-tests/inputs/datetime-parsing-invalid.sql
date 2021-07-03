--- TESTS FOR DATETIME PARSING FUNCTIONS WITH INVALID VALUES ---

-- parsing invalid value with pattern 'y'
select to_timestamp('294248', 'y'); -- out of year value range [0, 294247]
select to_timestamp('1', 'yy'); -- the number of digits must be 2 for 'yy'.
select to_timestamp('-12', 'yy'); -- out of year value range [0, 99] for reduced two digit form
select to_timestamp('123', 'yy'); -- the number of digits must be 2 for 'yy'.
select to_timestamp('1', 'yyy'); -- the number of digits must be in [3, 6] for 'yyy'

select to_timestamp('1234567', 'yyyyyyy'); -- the length of 'y' pattern must be less than 7

-- parsing invalid values with pattern 'D'
select to_timestamp('366', 'D');
select to_timestamp('9', 'DD');
-- in java 8 this case is invalid, but valid in java 11, disabled for jenkins
-- select to_timestamp('100', 'DD');
select to_timestamp('366', 'DD');
select to_timestamp('9', 'DDD');
select to_timestamp('99', 'DDD');
select to_timestamp('30-365', 'dd-DDD');
select to_timestamp('11-365', 'MM-DDD');
select to_timestamp('2019-366', 'yyyy-DDD');
select to_timestamp('12-30-365', 'MM-dd-DDD');
select to_timestamp('2020-01-365', 'yyyy-dd-DDD');
select to_timestamp('2020-10-350', 'yyyy-MM-DDD');
select to_timestamp('2020-11-31-366', 'yyyy-MM-dd-DDD');
-- add a special case to test csv, because the legacy formatter it uses is lenient then Spark should
-- throw SparkUpgradeException
select from_csv('2018-366', 'date Date', map('dateFormat', 'yyyy-DDD'))
