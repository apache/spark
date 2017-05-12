
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

-- check operator precedence.
-- We follow Oracle operator precedence in the table below that lists the levels of precedence
-- among SQL operators from high to low:
------------------------------------------------------------------------------------------
-- Operator                                          Operation
------------------------------------------------------------------------------------------
-- +, -                                              identity, negation
-- *, /                                              multiplication, division
-- +, -, ||                                          addition, subtraction, concatenation
-- =, !=, <, >, <=, >=, IS NULL, LIKE, BETWEEN, IN   comparison
-- NOT                                               exponentiation, logical negation
-- AND                                               conjunction
-- OR                                                disjunction
------------------------------------------------------------------------------------------
explain select 'a' || 1 + 2;
explain select 1 - 2 || 'b';
explain select 2 * 4  + 3 || 'b';
explain select 3 + 1 || 'a' || 4 / 2;
explain select 1 == 1 OR 'a' || 'b' ==  'ab';
explain select 'a' || 'c' == 'ac' AND 2 == 3;
