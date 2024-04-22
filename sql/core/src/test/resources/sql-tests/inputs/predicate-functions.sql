-- NOT
select not true;
select ! true;
select not null::boolean;

-- AND
select true and true;
select true and false;
select false and true;
select false and false;
select true and null::boolean;
select false and null::boolean;
select null::boolean and true;
select null::boolean and false;
select null::boolean and null::boolean;

-- OR
select true or true;
select true or false;
select false or true;
select false or false;
select true or null::boolean;
select false or null::boolean;
select null::boolean or true;
select null::boolean or false;
select null::boolean or null::boolean;

-- EqualTo
select 1 = 1;
select 1 = '1';
select 1.0 = '1';
select 1.5 = '1.51';

-- GreaterThan
select 1 > '1';
select 2 > '1.0';
select 2 > '2.0';
select 2 > '2.2';
select '1.5' > 0.5;
select to_date('2009-07-30 04:17:52') > to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') > '2009-07-30 04:17:52';
 
-- GreaterThanOrEqual
select 1 >= '1';
select 2 >= '1.0';
select 2 >= '2.0';
select 2.0 >= '2.2';
select '1.5' >= 0.5;
select to_date('2009-07-30 04:17:52') >= to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') >= '2009-07-30 04:17:52';
 
-- LessThan
select 1 < '1';
select 2 < '1.0';
select 2 < '2.0';
select 2.0 < '2.2';
select 0.5 < '1.5';
select to_date('2009-07-30 04:17:52') < to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') < '2009-07-30 04:17:52';
 
-- LessThanOrEqual
select 1 <= '1';
select 2 <= '1.0';
select 2 <= '2.0';
select 2.0 <= '2.2';
select 0.5 <= '1.5';
select to_date('2009-07-30 04:17:52') <= to_date('2009-07-30 04:17:52');
select to_date('2009-07-30 04:17:52') <= '2009-07-30 04:17:52';

-- SPARK-23549: Cast to timestamp when comparing timestamp with date
select to_date('2017-03-01') = to_timestamp('2017-03-01 00:00:00');
select to_timestamp('2017-03-01 00:00:01') > to_date('2017-03-01');
select to_timestamp('2017-03-01 00:00:01') >= to_date('2017-03-01');
select to_date('2017-03-01') < to_timestamp('2017-03-01 00:00:01');
select to_date('2017-03-01') <= to_timestamp('2017-03-01 00:00:01');

-- In
select 1 in (1, 2, 3);
select 1 in (1, 2, 3, null);
select 1 in (1.0, 2.0, 3.0);
select 1 in (1.0, 2.0, 3.0, null);
select 1 in ('2', '3', '4');
select 1 in ('2', '3', '4', null);
select null in (1, 2, 3);
select null in (1, 2, null);

-- Not(In)
select 1 not in (1, 2, 3);
select 1 not in (1, 2, 3, null);
select 1 not in (1.0, 2.0, 3.0);
select 1 not in (1.0, 2.0, 3.0, null);
select 1 not in ('2', '3', '4');
select 1 not in ('2', '3', '4', null);
select null not in (1, 2, 3);
select null not in (1, 2, null);

-- Between
select 1 between 0 and 2;
select 0.5 between 0 and 1;
select 2.0 between '1.0' and '3.0';
select 'b' between 'a' and 'c';
select to_timestamp('2022-12-26 00:00:01') between to_date('2022-03-01') and to_date('2022-12-31');
select rand(123) between 0.1 AND 0.2;

-- Not(Between)
select 1 not between 0 and 2;
select 0.5 not between 0 and 1;
select 2.0 not between '1.0' and '3.0';
select 'b' not between 'a' and 'c';
select to_timestamp('2022-12-26 00:00:01') not between to_date('2022-03-01') and to_date('2022-12-31');
select rand(123) not between 0.1 AND 0.2;

-- Sanity test for legacy flag equating ! with NOT
set spark.sql.legacy.bangEqualsNot=true;
select 1 ! between 0 and 2;
select 1 ! in (3, 4);
select 'hello' ! like 'world';
select 1 is ! null;
select false is ! true;
set spark.sql.legacy.bangEqualsNot=false;
