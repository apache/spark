
-- unary minus and plus
select -100;
select +230;
select -5.2;
select +6.8e0;
select -key, +key from testdata where key = 2;
select -(key + 1), - key + 1, +(key + 5) from testdata where key = 1;
select -max(key), +max(key) from testdata;
select - (-10);
select + (-key) from testdata where key = 32;
select - (+max(key)) from testdata;
select - - 3;
select - + 20;
select + + 100;
select - - max(key) from testdata;
select + - key from testdata where key = 33;

-- div
select 5 / 2;
select 5 / 0;
select 5 / null;
select null / 5;
select 5 div 2;
select 5 div 0;
select 5 div null;
select null div 5;

-- other arithmetics
select 1 + 2;
select 1 - 2;
select 2 * 5;
select 5 % 3;
select pmod(-7, 3);

-- check operator precedence
EXPLAIN SELECT 'a' || 1 + 2;
EXPLAIN SELECT 1 - 2 || 'b';
EXPLAIN SELECT 2 * 4  + 3 || 'b';
EXPLAIN SELECT 3 + 1 || 'a' || 4 / 2;
EXPLAIN SELECT 1 == 1 OR 'a' || 'b' ==  'ab';
EXPLAIN SELECT 'a' || 'c' == 'ac' AND 2 == 3;
