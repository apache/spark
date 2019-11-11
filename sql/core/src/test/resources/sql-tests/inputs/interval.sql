-- test for intervals

-- greater than or equal
select interval '1 day' > interval '23 hour';
select interval '-1 day' >= interval '-23 hour';
select interval '-1 day' > null;
select null > interval '-1 day';

-- less than or equal
select interval '1 minutes' < interval '1 hour';
select interval '-1 day' <= interval '-23 hour';

-- equal
select interval '1 year' = interval '360 days';
select interval '1 year 2 month' = interval '420 days';
select interval '1 year' = interval '365 days';
select interval '1 month' = interval '30 days';
select interval '1 minutes' = interval '1 hour';
select interval '1 minutes' = null;
select null = interval '-1 day';

-- null safe equal
select interval '1 minutes' <=> null;
select null <=> interval '1 minutes';

-- complex interval representation
select INTERVAL '9 years 1 months -1 weeks -4 days -10 hours -46 minutes' > interval '1 minutes';

-- ordering
select cast(v as interval) i from VALUES ('1 seconds'), ('4 seconds'), ('3 seconds') t(v) order by i;

-- unlimited days
select interval '1 month 120 days' > interval '2 month';
select interval '1 month 30 days' = interval '2 month';

-- unlimited microseconds
select interval '1 month 29 days 40 hours' > interval '2 month';

-- max
select max(cast(v as interval)) from VALUES ('1 seconds'), ('4 seconds'), ('3 seconds') t(v);

-- min
select min(cast(v as interval)) from VALUES ('1 seconds'), ('4 seconds'), ('3 seconds') t(v);

-- multiply and divide an interval by a number
select 3 * (timestamp'2019-10-15 10:11:12.001002' - date'2019-10-15');
select interval 4 month 2 weeks 3 microseconds * 1.5;
select (timestamp'2019-10-15' - timestamp'2019-10-14') / 1.5;

-- interval operation with null and zero case
select interval '2 seconds' / 0;
select interval '2 seconds' / null;
select interval '2 seconds' * null;
select null * interval '2 seconds';

-- interval with a negative sign
select -interval '-1 month 1 day -1 second';
select -interval -1 month 1 day -1 second;

-- make intervals
select make_interval(1);
select make_interval(1, 2);
select make_interval(1, 2, 3);
select make_interval(1, 2, 3, 4);
select make_interval(1, 2, 3, 4, 5);
select make_interval(1, 2, 3, 4, 5, 6);
select make_interval(1, 2, 3, 4, 5, 6, 7.008009);

-- abbreviations support for interval multi unit string
select interval '1 years 2 months 3 weeks 4 days 5 hours 6 minutes 7 seconds 8 milliseconds 9 microseconds';
select interval '1 year 2 month 3 week 4 day 5 hour 6 minute 7 second 8 millisecond 9 microsecond';
select interval '1 y 2 mon 3 w 4 d 5 h 6 m 7 s 8 ms 9 us';
select interval '1 y 2 yr -1 yrs 3 years -2 mons - 3 w  + 4 d +5 hrs 6 mins 7 secs 8 msecs 9 usecs';
select interval 'day 1 y';
select interval '1 1 y';
select interval '1 1 1 y';
select interval '1 daY';
select interval '';
select interval ' ';
select interval '-';
select interval '- year';
select interval '-1 yea';

-- abbreviations not cover these which require units as keywords in sql parser
select interval 1 y 2 mon 3 week;
select interval '30' y '25' mon;
select interval '-1 yrs' year to month;
