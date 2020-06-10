--- TESTS FOR DATETIME PARSING FUNCTIONS WITH INVALID VALUES ---

select to_timestamp('10', 'D');
select to_timestamp('100', 'DD');
select to_timestamp('366', 'DD');
select to_timestamp('30-365', 'dd-DDD');
select to_timestamp('11-365', 'MM-DDD');
select to_timestamp('2019-366', 'yyyy-DDD');
select to_timestamp('12-30-365', 'MM-dd-DDD');
select to_timestamp('2020-01-365', 'yyyy-dd-DDD');
select to_timestamp('2020-10-350', 'yyyy-MM-DDD');
select to_timestamp('2020-11-31-366', 'yyyy-MM-dd-DDD');
