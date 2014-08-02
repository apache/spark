create table tmp_srcpart like srcpart;

insert overwrite table tmp_srcpart partition (ds='2008-04-08', hr='11')
select key, value from srcpart where ds='2008-04-08' and hr='11';

explain
create table tmp_unionall as
SELECT count(1) as counts, key, value
FROM
(
  SELECT key, value FROM srcpart a WHERE a.ds='2008-04-08' and a.hr='11'

    UNION ALL

  SELECT key, key as value FROM (
    SELECT distinct key FROM (
      SELECT key, value FROM tmp_srcpart a WHERE a.ds='2008-04-08' and a.hr='11'
        UNION ALL
      SELECT key, value FROM tmp_srcpart b WHERE b.ds='2008-04-08' and b.hr='11'
    )t
  ) master_table
) a GROUP BY key, value
;
