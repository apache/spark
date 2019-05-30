CREATE TEMPORARY VIEW strings(s1, s2, s3, s4, s5, s6, s7) AS VALUES (
  null,
  '2019',
  '2019 05',
  '2019 05 28',
  '2019 05 28 17 03 53',
  '2019 05 28 17 03 53 123',
  '2019 05 28 17 03 53 123456'
);

SELECT
    CAST(s1 AS DATE FORMAT 'yyyy'),
    CAST(s2 AS DATE FORMAT 'yyyy'),
    CAST(s3 AS DATE FORMAT 'yyyy MM'),
    CAST(s4 AS DATE FORMAT 'yyyy MM dd'),
    CAST(s2 AS DATE FORMAT 'invalid')
FROM strings;

SELECT
    CAST(s1 AS TIMESTAMP FORMAT 'yyyy'),
    CAST(s2 AS TIMESTAMP FORMAT 'yyyy'),
    CAST(s3 AS TIMESTAMP FORMAT 'yyyy MM'),
    CAST(s4 AS TIMESTAMP FORMAT 'yyyy MM dd'),
    CAST(s5 AS TIMESTAMP FORMAT 'yyyy MM dd HH mm ss'),
    CAST(s6 AS TIMESTAMP FORMAT 'yyyy MM dd HH mm ss SSS'),
    CAST(s7 AS TIMESTAMP FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
    CAST(s2 AS TIMESTAMP FORMAT 'invalid')
FROM strings;

CREATE TEMPORARY VIEW dates(d1, d2) AS
SELECT null, to_date('2019-05-28');

SELECT
    CAST(d1 AS STRING FORMAT 'yyyy'),
    CAST(d2 AS STRING FORMAT 'yyyy'),
    CAST(d2 AS STRING FORMAT 'yyyy MM'),
    CAST(d2 AS STRING FORMAT 'yyyy MM dd'),
    CAST(d2 AS STRING FORMAT 'invalid')
FROM dates;

CREATE TEMPORARY VIEW timestamps(ts1, ts2) AS
SELECT null, to_timestamp('2019-05-28 17:03:53.123456');

SELECT
    CAST(ts1 AS STRING FORMAT 'yyyy'),
    CAST(ts2 AS STRING FORMAT 'yyyy'),
    CAST(ts2 AS STRING FORMAT 'yyyy MM'),
    CAST(ts2 AS STRING FORMAT 'yyyy MM dd'),
    CAST(ts2 AS STRING FORMAT 'yyyy MM dd HH mm ss'),
    CAST(ts2 AS STRING FORMAT 'yyyy MM dd HH mm ss SSS'),
    CAST(ts2 AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
    CAST(ts2 AS STRING FORMAT 'invalid')
FROM timestamps;
