-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
(null, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
(1, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "a"),
(1, 2L, 2.5D, date("2017-08-02"), timestamp_seconds(1502000000), "a"),
(2, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "a"),
(1, null, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), "b"),
(2, 3L, 3.3D, date("2017-08-03"), timestamp_seconds(1503000000), "b"),
(3, 2147483650L, 100.001D, date("2020-12-31"), timestamp_seconds(1609372800), "b"),
(null, null, null, null, null, null),
(3, 1L, 1.0D, date("2017-08-01"), timestamp_seconds(1501545600), null)
AS testData(val, val_long, val_double, val_date, val_timestamp, cate);

-- Test QUALIFY clause
SELECT key,
       max(val) OVER (partition BY val_date) AS m
FROM   testdata
WHERE  key > 2
QUALIFY m > 2 AND m < 10;

SELECT val_long,
       val_date,
       val
FROM   testdata QUALIFY max(val) OVER (partition BY val_date) >= 3;

SELECT val_date,
       val * sum(val) OVER (partition BY val_date) AS w
FROM   testdata
QUALIFY w > 10;

SELECT   w.val_date
FROM     testdata w
JOIN     testdata w2 ON w.val_date=w2.val_date
QUALIFY row_number() OVER (partition BY w.val_date ORDER BY w.val) IN (2);

SELECT   val_date,
         count(val_long) OVER (partition BY val_date) AS w
FROM     testdata
GROUP BY val_date,
         val_long
HAVING   Sum(val) > 1
QUALIFY w = 1;

SELECT   val_date,
         val_long,
         Sum(val)
FROM     testdata
GROUP BY val_date,
         val_long
HAVING   Sum(val) > 1
QUALIFY count(val_long) OVER (partition BY val_date) IN(SELECT 1);

SELECT   val_date,
         val_long
FROM     testdata
QUALIFY count(val_long) OVER (partition BY val_date) > 1 AND val > 1;

SELECT   val_date,
         val_long
FROM     testdata
QUALIFY val > 1;

SELECT   val_date,
         val_long
FROM     testdata
GROUP BY val_date,
         val_long
QUALIFY w = 1 AND sum(val) > 1;
