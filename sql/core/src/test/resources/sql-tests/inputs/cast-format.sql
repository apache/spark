SELECT CAST('2019-05-28' AS DATE FORMAT);

CREATE TEMPORARY VIEW strings(snull, sdate, stimestamp, sinvalid) AS VALUES (
  null,
  '2019 05 28',
  '2019 05 28 17 03 53 123456',
  'invalid'
);

SELECT
    CAST(snull AS DATE FORMAT 'yyyy MM dd'),
    CAST(sdate AS DATE FORMAT 'yyyy MM dd'),
    CAST(sdate AS DATE FORMAT 'invalid'),
    CAST(sdate AS DATE FORMAT ''),
    CAST(sinvalid AS DATE FORMAT 'yyyy MM dd')
FROM strings;

SELECT
    CAST(snull AS TIMESTAMP FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
    CAST(stimestamp AS TIMESTAMP FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
    CAST(stimestamp AS TIMESTAMP FORMAT 'invalid'),
    CAST(stimestamp AS TIMESTAMP FORMAT ''),
    CAST(sinvalid AS TIMESTAMP FORMAT 'yyyy MM dd HH mm ss SSSSSS')
FROM strings;

CREATE TEMPORARY VIEW dates(dnull, ddate) AS
SELECT null, to_date('2019-05-28');

SELECT
    CAST(dnull AS STRING FORMAT 'yyyy MM dd'),
    CAST(ddate AS STRING FORMAT 'yyyy MM dd'),
    CAST(ddate AS STRING FORMAT 'invalid'),
    CAST(ddate AS STRING FORMAT '')
FROM dates;

CREATE TEMPORARY VIEW timestamps(tsnull, tsdate) AS
SELECT null, to_timestamp('2019-05-28 17:03:53.123456');

SELECT
    CAST(tsnull AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
    CAST(tsdate AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
    CAST(tsdate AS STRING FORMAT 'invalid'),
    CAST(tsdate AS STRING FORMAT '')
FROM timestamps;
