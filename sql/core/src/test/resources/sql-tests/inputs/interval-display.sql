-- tests for interval output style

SELECT
  cast(null as interval), -- null
  interval '0 day', -- 0
  interval '1 year', -- year only
  interval '1 month', -- month only
  interval '1 year 2 month', -- year month only
  interval '1 day -1 hours',
  interval '-1 day -1 hours',
  interval '-1 day 1 hours',
  interval '-1 days +1 hours',
  interval '1 years 2 months -3 days 4 hours 5 minutes 6.789 seconds',
  - interval '1 years 2 months -3 days 4 hours 5 minutes 6.789 seconds';
