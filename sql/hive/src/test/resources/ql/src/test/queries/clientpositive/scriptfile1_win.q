set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
-- INCLUDE_OS_WINDOWS

CREATE TABLE dest1(key INT, value STRING);

ADD FILE src/test/scripts/testgrep_win.bat;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'testgrep_win.bat' AS (tkey, tvalue)
  CLUSTER BY tkey
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue;

SELECT dest1.* FROM dest1;
