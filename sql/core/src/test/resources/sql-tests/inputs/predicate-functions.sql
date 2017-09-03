-- EqualTo
select 1 = 1;
select 1 = '1';
select 1.0 = '1';

-- GreaterThan
select 1 > '1';
select 2 > '1.0';
select 2 > '2.0';
select 2 > '2.2';
select to_date('2009-07-30 04:17:52') > to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') > '2009-07-30 04:17:52';
 
-- GreaterThanOrEqual
select 1 >= '1';
select 2 >= '1.0';
select 2 >= '2.0';
select 2.0 >= '2.2';
select to_date('2009-07-30 04:17:52') >= to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') >= '2009-07-30 04:17:52';
 
-- LessThan
select 1 < '1';
select 2 < '1.0';
select 2 < '2.0';
select 2.0 < '2.2';
select to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') < '2009-07-30 04:17:52';
 
-- LessThanOrEqual
select 1 <= '1';
select 2 <= '1.0';
select 2 <= '2.0';
select 2.0 <= '2.2';
select to_date('2009-07-30 04:17:52') <= to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') <= '2009-07-30 04:17:52';
