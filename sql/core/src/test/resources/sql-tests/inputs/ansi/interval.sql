--IMPORT interval.sql

-- the `interval` keyword can be omitted with ansi mode
select 1 year 2 days;
select '10-9' year to month;
select '20 15:40:32.99899999' day to second;
select 30 day day;
select date'2012-01-01' - '2-2' year to month;
select 1 month - 1 day;

-- malformed interval literal with ansi mode
select 1 year to month;
select '1' year to second;
select 1 year '2-1' year to month;
select (-30) day;
select (a + 1) day;
select 30 day day day;