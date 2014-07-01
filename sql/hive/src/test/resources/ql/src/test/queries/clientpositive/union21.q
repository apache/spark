-- union of constants, udf outputs, and columns from text table and thrift table

explain
SELECT key, count(1)
FROM (
  SELECT '1' as key from src
  UNION ALL
  SELECT reverse(key) as key from src
  UNION ALL
  SELECT key as key from src
  UNION ALL
  SELECT astring as key from src_thrift
  UNION ALL
  SELECT lstring[0] as key from src_thrift
) union_output
GROUP BY key;

SELECT key, count(1)
FROM (
  SELECT '1' as key from src
  UNION ALL
  SELECT reverse(key) as key from src
  UNION ALL
  SELECT key as key from src
  UNION ALL
  SELECT astring as key from src_thrift
  UNION ALL
  SELECT lstring[0] as key from src_thrift
) union_output
GROUP BY key;
