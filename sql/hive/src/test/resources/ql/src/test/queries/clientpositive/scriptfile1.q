set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- EXCLUDE_OS_WINDOWS
CREATE TABLE dest1(key INT, value STRING);

ADD FILE ../../ql/src/test/scripts/testgrep;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'testgrep' AS (tkey, tvalue)
  CLUSTER BY tkey
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tmap.tkey, tmap.tvalue;

SELECT dest1.* FROM dest1;
