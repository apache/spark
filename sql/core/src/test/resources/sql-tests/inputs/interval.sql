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

-- make intervals
select make_interval(1);
select make_interval(1, 2);
select make_interval(1, 2, 3);
select make_interval(1, 2, 3, 4);
select make_interval(1, 2, 3, 4, 5);
select make_interval(1, 2, 3, 4, 5, 6);
select make_interval(1, 2, 3, 4, 5, 6, 7.008009);
