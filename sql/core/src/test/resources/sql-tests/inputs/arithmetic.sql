
-- unary minus and plus
select -100;
select +230;
select -5.2;
select +6.8e0;
select -key, +key from uniqueRowData where key = 2;
select -(key + 1), - key + 1, +(key + 5) from uniqueRowData where key = 1;
select -max(key), +max(key) from uniqueRowData;
select - (-10);
select + (-key) from uniqueRowData where key = 32;
select - (+max(key)) from uniqueRowData;
select - - 3;
select - + 20;
select + + 100;
select - - max(key) from uniqueRowData;
select + - key from uniqueRowData where key = 33;

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
