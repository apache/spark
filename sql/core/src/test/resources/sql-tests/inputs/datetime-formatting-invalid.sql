--- TESTS FOR DATETIME FORMATTING FUNCTIONS WITH INVALID PATTERNS ---

-- separating this from datetime-formatting.sql, because the text form
-- for patterns with 5 letters in SimpleDateFormat varies from different JDKs
select date_format('2018-11-17 13:33:33.333', 'GGGGG');
-- pattern letter count can not be greater than 6
select date_format('2018-11-17 13:33:33.333', 'yyyyyyy');
-- q/L in JDK 8 will fail when the count is more than 2
select date_format('2018-11-17 13:33:33.333', 'qqqqq');
select date_format('2018-11-17 13:33:33.333', 'QQQQQ');
select date_format('2018-11-17 13:33:33.333', 'MMMMM');
select date_format('2018-11-17 13:33:33.333', 'LLLLL');

select date_format('2018-11-17 13:33:33.333', 'EEEEE');
select date_format('2018-11-17 13:33:33.333', 'FF');
select date_format('2018-11-17 13:33:33.333', 'ddd');
-- DD is invalid if the day-of-year exceeds 100, but it becomes valid in Java 11
-- select date_format('2018-11-17 13:33:33.333', 'DD');
select date_format('2018-11-17 13:33:33.333', 'DDDD');
select date_format('2018-11-17 13:33:33.333', 'HHH');
select date_format('2018-11-17 13:33:33.333', 'hhh');
select date_format('2018-11-17 13:33:33.333', 'kkk');
select date_format('2018-11-17 13:33:33.333', 'KKK');
select date_format('2018-11-17 13:33:33.333', 'mmm');
select date_format('2018-11-17 13:33:33.333', 'sss');
select date_format('2018-11-17 13:33:33.333', 'SSSSSSSSSS');
select date_format('2018-11-17 13:33:33.333', 'aa');
select date_format('2018-11-17 13:33:33.333', 'V');
select date_format('2018-11-17 13:33:33.333', 'zzzzz');
select date_format('2018-11-17 13:33:33.333', 'XXXXXX');
select date_format('2018-11-17 13:33:33.333', 'ZZZZZZ');
select date_format('2018-11-17 13:33:33.333', 'OO');
select date_format('2018-11-17 13:33:33.333', 'xxxxxx');

select date_format('2018-11-17 13:33:33.333', 'A');
select date_format('2018-11-17 13:33:33.333', 'n');
select date_format('2018-11-17 13:33:33.333', 'N');
select date_format('2018-11-17 13:33:33.333', 'p');

-- disabled week-based patterns
select date_format('2018-11-17 13:33:33.333', 'Y');
select date_format('2018-11-17 13:33:33.333', 'w');
select date_format('2018-11-17 13:33:33.333', 'W');
select date_format('2018-11-17 13:33:33.333', 'u');
select date_format('2018-11-17 13:33:33.333', 'e');
select date_format('2018-11-17 13:33:33.333', 'c');

-- others
select date_format('2018-11-17 13:33:33.333', 'B');
select date_format('2018-11-17 13:33:33.333', 'C');
select date_format('2018-11-17 13:33:33.333', 'I');


