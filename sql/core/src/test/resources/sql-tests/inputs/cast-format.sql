-- Analysis fails without pattern if format is defined
SELECT CAST('2019-05-28' AS DATE FORMAT);

-- String to date and timestamp tests
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


-- Format is not applied on embedded string type
CREATE TEMPORARY VIEW stringarrays(sadate, satimestamp) AS VALUES (
  array('2019-05-28'),
  array('2019-05-28 17:03:53.123456')
);

SELECT
  CAST(sadate AS ARRAY<DATE> FORMAT 'yyyy MM dd'),
  CAST(satimestamp AS ARRAY<TIMESTAMP> FORMAT 'yyyy MM dd HH mm ss SSSSSS')
FROM stringarrays;

CREATE TEMPORARY VIEW stringmaps(smdate, smtimestamp) AS VALUES (
  map('2019-05-28', '2019-05-28'),
  map('2019-05-28 17:03:53.123456', '2019-05-28 17:03:53.123456')
);

SELECT
  CAST(smdate AS MAP<STRING, DATE> FORMAT 'yyyy MM dd'),
  CAST(smtimestamp AS MAP<STRING, TIMESTAMP> FORMAT 'yyyy MM dd HH mm ss SSSSSS')
FROM stringmaps;

CREATE TEMPORARY VIEW stringstructs(ssdate, sstimestamp) AS VALUES (
  named_struct('field', '2019-05-28'),
  named_struct('field', '2019-05-28 17:03:53.123456')
);

SELECT
  CAST(ssdate AS STRUCT<field:DATE> FORMAT 'yyyy MM dd'),
  CAST(sstimestamp AS STRUCT<field:TIMESTAMP> FORMAT 'yyyy MM dd HH mm ss SSSSSS')
FROM stringstructs;


-- Date to string tests
CREATE TEMPORARY VIEW dates(dnull, ddate) AS
SELECT null, to_date('2019-05-28');

SELECT
  CAST(dnull AS STRING FORMAT 'yyyy MM dd'),
  CAST(ddate AS STRING FORMAT 'yyyy MM dd'),
  CAST(ddate AS STRING FORMAT 'invalid'),
  CAST(ddate AS STRING FORMAT '')
FROM dates;


-- Format is not applied on embedded date type
CREATE TEMPORARY VIEW datearrays(da) AS
SELECT array(to_date('2019-05-28'));

SELECT
  CAST(da AS STRING FORMAT 'yyyy MM dd')
FROM datearrays;

CREATE TEMPORARY VIEW datemaps(dm) AS
SELECT map(to_date('2019-05-29'), to_date('2019-05-28'));

SELECT
  CAST(dm AS STRING FORMAT 'yyyy MM dd')
FROM datemaps;

CREATE TEMPORARY VIEW datestructs(ds) AS
SELECT named_struct('field', to_date('2019-05-28'));

SELECT
  CAST(ds AS STRING FORMAT 'yyyy MM dd')
FROM datestructs;


-- Timestamp to string tests
CREATE TEMPORARY VIEW timestamps(tsnull, tsdate) AS
SELECT null, to_timestamp('2019-05-28 17:03:53.123456');

SELECT
  CAST(tsnull AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
  CAST(tsdate AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS'),
  CAST(tsdate AS STRING FORMAT 'invalid'),
  CAST(tsdate AS STRING FORMAT '')
FROM timestamps;


-- Format is not applied on embedded timestamp type
CREATE TEMPORARY VIEW timestamparrays(tsa) AS
SELECT array(to_timestamp('2019-05-28 17:03:53.123456'));

SELECT
  CAST(tsa AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS')
FROM timestamparrays;

CREATE TEMPORARY VIEW timestampmaps(tsm) AS
SELECT map(to_timestamp('2019-05-28 17:03:53.123456'), to_timestamp('2019-05-28 17:03:53.123456'));

SELECT
  CAST(tsm AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS')
FROM timestampmaps;

CREATE TEMPORARY VIEW timestampstructs(tss) AS
SELECT named_struct('field', to_timestamp('2019-05-28 17:03:53.123456'));

SELECT
  CAST(tss AS STRING FORMAT 'yyyy MM dd HH mm ss SSSSSS')
FROM timestampstructs;
