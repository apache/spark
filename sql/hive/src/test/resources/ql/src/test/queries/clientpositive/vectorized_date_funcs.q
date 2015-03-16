SET hive.vectorized.execution.enabled = true;

-- Test timestamp functions in vectorized mode to verify they run correctly end-to-end.

CREATE TABLE date_udf_flight (
  origin_city_name STRING,
  dest_city_name STRING,
  fl_date DATE,
  arr_delay FLOAT,
  fl_num INT
);
LOAD DATA LOCAL INPATH '../../data/files/flights_tiny.txt.1' OVERWRITE INTO TABLE date_udf_flight;

CREATE TABLE date_udf_flight_orc (
  fl_date DATE,
  fl_time TIMESTAMP
) STORED AS ORC;

INSERT INTO TABLE date_udf_flight_orc SELECT fl_date, to_utc_timestamp(fl_date, 'America/Los_Angeles') FROM date_udf_flight;

SELECT * FROM date_udf_flight_orc;

EXPLAIN SELECT
  to_unix_timestamp(fl_time),
  year(fl_time),
  month(fl_time),
  day(fl_time),
  dayofmonth(fl_time),
  weekofyear(fl_time),
  date(fl_time),
  to_date(fl_time),
  date_add(fl_time, 2),
  date_sub(fl_time, 2),
  datediff(fl_time, "2000-01-01")
FROM date_udf_flight_orc;

SELECT
  to_unix_timestamp(fl_time),
  year(fl_time),
  month(fl_time),
  day(fl_time),
  dayofmonth(fl_time),
  weekofyear(fl_time),
  date(fl_time),
  to_date(fl_time),
  date_add(fl_time, 2),
  date_sub(fl_time, 2),
  datediff(fl_time, "2000-01-01")
FROM date_udf_flight_orc;

EXPLAIN SELECT
  to_unix_timestamp(fl_date),
  year(fl_date),
  month(fl_date),
  day(fl_date),
  dayofmonth(fl_date),
  weekofyear(fl_date),
  date(fl_date),
  to_date(fl_date),
  date_add(fl_date, 2),
  date_sub(fl_date, 2),
  datediff(fl_date, "2000-01-01")
FROM date_udf_flight_orc;

SELECT
  to_unix_timestamp(fl_date),
  year(fl_date),
  month(fl_date),
  day(fl_date),
  dayofmonth(fl_date),
  weekofyear(fl_date),
  date(fl_date),
  to_date(fl_date),
  date_add(fl_date, 2),
  date_sub(fl_date, 2),
  datediff(fl_date, "2000-01-01")
FROM date_udf_flight_orc;

EXPLAIN SELECT
  year(fl_time) = year(fl_date),
  month(fl_time) = month(fl_date),
  day(fl_time) = day(fl_date),
  dayofmonth(fl_time) = dayofmonth(fl_date),
  weekofyear(fl_time) = weekofyear(fl_date),
  date(fl_time) = date(fl_date),
  to_date(fl_time) = to_date(fl_date),
  date_add(fl_time, 2) = date_add(fl_date, 2),
  date_sub(fl_time, 2) = date_sub(fl_date, 2),
  datediff(fl_time, "2000-01-01") = datediff(fl_date, "2000-01-01")
FROM date_udf_flight_orc;

-- Should all be true or NULL
SELECT
  year(fl_time) = year(fl_date),
  month(fl_time) = month(fl_date),
  day(fl_time) = day(fl_date),
  dayofmonth(fl_time) = dayofmonth(fl_date),
  weekofyear(fl_time) = weekofyear(fl_date),
  date(fl_time) = date(fl_date),
  to_date(fl_time) = to_date(fl_date),
  date_add(fl_time, 2) = date_add(fl_date, 2),
  date_sub(fl_time, 2) = date_sub(fl_date, 2),
  datediff(fl_time, "2000-01-01") = datediff(fl_date, "2000-01-01")
FROM date_udf_flight_orc;

EXPLAIN SELECT 
  fl_date, 
  to_date(date_add(fl_date, 2)), 
  to_date(date_sub(fl_date, 2)),
  datediff(fl_date, date_add(fl_date, 2)), 
  datediff(fl_date, date_sub(fl_date, 2)),
  datediff(date_add(fl_date, 2), date_sub(fl_date, 2)) 
FROM date_udf_flight_orc LIMIT 10;

SELECT 
  fl_date, 
  to_date(date_add(fl_date, 2)), 
  to_date(date_sub(fl_date, 2)),
  datediff(fl_date, date_add(fl_date, 2)), 
  datediff(fl_date, date_sub(fl_date, 2)),
  datediff(date_add(fl_date, 2), date_sub(fl_date, 2)) 
FROM date_udf_flight_orc LIMIT 10;
